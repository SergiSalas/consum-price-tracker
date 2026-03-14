import requests
import sqlite3
import time
import os
import csv
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────

BASE_URL  = "https://tienda.consum.es/api/rest/V1.0"
DB_PATH   = "data/consum_prices.db"
PAGE_SIZE = 100    # productos por petición
SLEEP_REQ = 0.15   # segundos entre páginas (cortesía con el servidor)

DEFAULT_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept":          "application/json",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer":         "https://tienda.consum.es/",
}

# ── ESTADO GLOBAL ─────────────────────────────────────────────────────────────
price_changes = []

# ── DB ────────────────────────────────────────────────────────────────────────

def init_db():
    print("📦 Inicializando base de datos...")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id          TEXT PRIMARY KEY,
            name        TEXT,
            brand       TEXT,
            last_price  REAL,
            unit_size   TEXT,
            category    TEXT,
            image_url   TEXT,
            last_update TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS price_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id  TEXT,
            name        TEXT,
            old_price   REAL,
            new_price   REAL,
            change_date TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    ''')
    conn.commit()
    conn.close()
    print("✅ Base de datos lista.")

# ── HTTP ──────────────────────────────────────────────────────────────────────

def safe_get(url, params=None, retries=3):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=DEFAULT_HEADERS, params=params, timeout=15)
            if r.status_code == 200:
                return r
            print(f"⚠️  HTTP {r.status_code} en {url} (intento {attempt}/{retries})")
        except requests.RequestException as e:
            print(f"❌ Error de red: {e} (intento {attempt}/{retries})")
        time.sleep(2)
    return None

# ── EXTRACCIÓN DE CAMPOS ──────────────────────────────────────────────────────

def extract_product_fields(p):
    """
    Extrae los campos del JSON de un producto Consum.

    Estructura confirmada:
      p["id"]                                            → ID numérico
      p["productData"]["name"]                           → nombre
      p["productData"]["brand"]["name"]                  → marca
      p["productData"]["imageURL"]                       → imagen principal
      p["priceData"]["prices"][0]["value"]["centAmount"] → precio en € (NO son céntimos, el nombre engaña)
      p["priceData"]["unitPriceUnitType"]                → unidad (ej: "1 Kg")
      p["categories"][0]["name"]                         → categoría principal
    """
    pid        = str(p.get("id", "")).strip()
    prod_data  = p.get("productData", {})
    price_data = p.get("priceData", {})

    name      = prod_data.get("name", "Sin nombre").strip()
    brand     = (prod_data.get("brand") or {}).get("name", "").strip()
    image_url = prod_data.get("imageURL", "").strip()

    # Precio — centAmount ya viene en euros (1.15 = 1.15€), nombre de campo engañoso
    try:
        price = float(price_data["prices"][0]["value"]["centAmount"])
    except (KeyError, IndexError, TypeError, ValueError):
        price = 0.0

    unit_size  = price_data.get("unitPriceUnitType", "").strip()
    categories = p.get("categories", [])
    category   = categories[0]["name"].strip() if categories else ""

    return pid, name, brand, price, unit_size, category, image_url

# ── PROCESADO DE PRODUCTO ─────────────────────────────────────────────────────

def process_product(p):
    pid, name, brand, new_price, unit_size, category, image_url = extract_product_fields(p)

    if not pid:
        return

    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    c.execute("SELECT last_price FROM products WHERE id = ?", (pid,))
    row = c.fetchone()

    if row:
        old_price = row[0]
        if old_price != new_price:
            arrow = "▲" if new_price > old_price else "▼"
            print(f"  {arrow} {name}: {old_price}€ → {new_price}€")
            price_changes.append((pid, name, old_price, new_price, now))
            c.execute('''
                INSERT INTO price_history (product_id, name, old_price, new_price, change_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (pid, name, old_price, new_price, now))
        # Actualiza siempre por si cambia imagen, marca, categoría, etc.
        c.execute('''
            UPDATE products
            SET name=?, brand=?, last_price=?, unit_size=?, category=?, image_url=?, last_update=?
            WHERE id=?
        ''', (name, brand, new_price, unit_size, category, image_url, now, pid))
    else:
        print(f"  ✅ Nuevo: {name} ({new_price}€)")
        c.execute('''
            INSERT INTO products (id, name, brand, last_price, unit_size, category, image_url, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (pid, name, brand, new_price, unit_size, category, image_url, now))

    conn.commit()
    conn.close()

# ── FETCH ALL PRODUCTS ────────────────────────────────────────────────────────

def fetch_all_products():
    """
    Pagina /catalog/product con currentPage.
    NOTA: la API ignora pageSize y devuelve ~20 productos fijos por página.
    Se para cuando:
      - no hay más productos en la respuesta
      - hasMore == False
      - total_seen >= totalCount (límite de seguridad anti-bucle infinito)
    """
    page        = 0
    total_seen  = 0
    total_count = None   # se rellena en la primera respuesta

    while True:
        params = {"currentPage": page}
        print(f"\n📄 Página {page} ({total_seen} procesados hasta ahora)...")

        response = safe_get(f"{BASE_URL}/catalog/product", params=params)
        if response is None:
            print(f"❌ No se pudo obtener la página {page}. Abortando.")
            break

        data     = response.json()
        products = data.get("products", [])

        # Guardar totalCount en la primera página
        if total_count is None:
            total_count = data.get("totalCount", 0)
            print(f"📊 Total de productos según API: {total_count}")

        if not products:
            print("✅ Sin más productos en esta página.")
            break

        for p in products:
            process_product(p)

        total_seen += len(products)
        print(f"   → {len(products)} procesados | acumulado: {total_seen} / {total_count}")

        # ── Condiciones de parada ──────────────────────────────────────────
        # 1. La API dice que no hay más
        if not data.get("hasMore", False):
            print(f"\n✅ hasMore=False. Total: {total_seen} productos.")
            break

        # 2. Seguridad: ya procesamos al menos todos los productos del totalCount
        if total_count and total_seen >= total_count:
            print(f"\n✅ Alcanzado totalCount ({total_count}). Deteniendo.")
            break

        page += 1
        time.sleep(SLEEP_REQ)

# ── EXPORT CSV ────────────────────────────────────────────────────────────────

def export_to_csv():
    os.makedirs("data_public", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    # products.csv
    with open("data_public/products.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "brand", "last_price", "unit_size", "category", "image_url", "last_update"])
        for row in c.execute("""
            SELECT id, name, brand, last_price, unit_size, category, image_url, last_update
            FROM products
            ORDER BY name COLLATE NOCASE
        """):
            w.writerow(row)

    # price_history.csv
    with open("data_public/price_history.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "product_id", "name", "old_price", "new_price", "change_date"])
        for row in c.execute("""
            SELECT id, product_id, name, old_price, new_price, change_date
            FROM price_history
            ORDER BY change_date DESC, id DESC
        """):
            w.writerow(row)

    conn.close()
    print("📤 CSVs exportados en data_public/")

# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    start = time.time()
    print("🚀 Iniciando rastreador de precios Consum...\n")

    try:
        init_db()
        fetch_all_products()
    finally:
        try:
            export_to_csv()
        except Exception as e:
            print(f"⚠️  Error al exportar CSV: {e}")

    elapsed = time.time() - start

    print(f"\n📌 Resumen de cambios ({len(price_changes)}):")
    if price_changes:
        for pid, name, old_p, new_p, _ in price_changes:
            arrow = "▲" if new_p > old_p else "▼"
            diff  = round(new_p - old_p, 2)
            print(f"  {arrow} {name}: {old_p}€ → {new_p}€  ({'+' if diff > 0 else ''}{diff}€)")
    else:
        print("  ✅ Sin cambios de precio en esta ejecución.")

    print(f"\n⏱️  Tiempo total: {elapsed:.2f}s")
    print("🏁 Proceso finalizado.")