import requests
import sqlite3
import time
import os
import csv
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────────────────────

BASE_URL  = "https://tienda.consum.es/api/rest/V1.0"
DB_PATH   = "data/consum_prices.db"
SLEEP_REQ = 0.15   # segundos entre páginas (cortesía con el servidor)

DEFAULT_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept":          "application/json",
    "Accept-Language": "es-ES,es;q=0.9",
    "Referer":         "https://tienda.consum.es/",
}

# ── ESTADO GLOBAL ──────────────────────────────────────────────────────────────────────────
rice_changes = []

# ── TAXONOMÍA CANÓNICA ─────────────────────────────────────────────────────────────────────
# Mapeo de keywords de las categorías de Consum (en español) a la categoría
# unificada compartida con BonPreu y Mercadona.
# Se usan las categorías con type==0 (reales), ignorando type==1 (promocionales).
# Se evalúan en orden: el primer match gana.

_CANONICAL_RULES: list[tuple[str, str]] = [
    # Lácteos — antes que derivados genéricos
    ("lácteo",           "Lacteos"),
    ("láctico",          "Lacteos"),
    ("leche",             "Lacteos"),
    ("yogur",             "Lacteos"),
    ("queso",             "Lacteos"),
    ("mantequilla",       "Lacteos"),
    ("nata ",             "Lacteos"),
    ("huevo",             "Lacteos"),   # Consum agrupa huevos con lácteos
    # Carnes
    ("carne",             "Carnes"),
    ("ave ",              "Carnes"),
    ("aves ",             "Carnes"),
    ("embutido",          "Carnes"),
    ("charcutería",      "Carnes"),
    ("jamón",             "Carnes"),
    ("pollo",             "Carnes"),
    # Pescados y Mariscos
    ("pescado",           "Pescados y Mariscos"),
    ("marisco",           "Pescados y Mariscos"),
    # Frutas y Verduras
    ("fruta",             "Frutas y Verduras"),
    ("verdura",           "Frutas y Verduras"),
    ("hortaliza",         "Frutas y Verduras"),
    # Panadería
    ("pan ",              "Panaderia y Bolleria"),
    ("bollería",          "Panaderia y Bolleria"),
    ("pastelería",        "Panaderia y Bolleria"),
    ("galleta",           "Panaderia y Bolleria"),
    # Congelados
    ("congelad",          "Congelados"),
    # Bebidas
    ("bebida",            "Bebidas"),
    ("cerveza",           "Bebidas"),
    ("vino",              "Bebidas"),
    ("cava",              "Bebidas"),
    ("agua ",             "Bebidas"),
    ("aguas ",            "Bebidas"),
    ("refresco",          "Bebidas"),
    ("zumo",              "Bebidas"),
    ("café",             "Bebidas"),
    ("cafe ",             "Bebidas"),
    ("infusión",          "Bebidas"),
    # Conservas
    ("conserva",          "Conservas"),
    ("enlatado",          "Conservas"),
    ("tarro",             "Conservas"),
    # Pasta, Arroz y Legumbres
    ("pasta",             "Pasta, Arroz y Legumbres"),
    ("arroz",             "Pasta, Arroz y Legumbres"),
    ("legumbre",          "Pasta, Arroz y Legumbres"),
    # Cereales y Desayunos
    ("cereal",            "Cereales y Desayunos"),
    ("desayuno",          "Cereales y Desayunos"),
    ("muesli",            "Cereales y Desayunos"),
    # Aceites y Condimentos
    ("aceite",            "Aceites y Condimentos"),
    ("vinagre",           "Aceites y Condimentos"),
    ("condimento",        "Aceites y Condimentos"),
    ("especia",           "Aceites y Condimentos"),
    ("salsa",             "Aceites y Condimentos"),
    ("sal ",              "Aceites y Condimentos"),
    # Snacks y Aperitivos
    ("aperitivo",         "Snacks y Aperitivos"),
    ("snack",             "Snacks y Aperitivos"),
    ("patatas fritas",    "Snacks y Aperitivos"),
    ("fruto seco",        "Snacks y Aperitivos"),
    # Dulces y Postres
    ("chocolate",         "Dulces y Postres"),
    ("dulce",             "Dulces y Postres"),
    ("postre",            "Dulces y Postres"),
    ("mermelada",         "Dulces y Postres"),
    (" miel ",            "Dulces y Postres"),
    # Higiene Personal
    ("higiene",           "Higiene Personal"),
    ("cuidado personal",  "Higiene Personal"),
    ("cosmética",         "Higiene Personal"),
    ("perfumería",        "Higiene Personal"),
    ("farmacia",          "Higiene Personal"),
    # Limpieza del Hogar
    ("limpieza",          "Limpieza del Hogar"),
    ("detergente",        "Limpieza del Hogar"),
    ("hogar",             "Limpieza del Hogar"),
    # Bebés y Niños
    ("bebé",              "Bebes y Ninos"),
    ("infantil",          "Bebes y Ninos"),
    # Mascotas
    ("mascota",           "Mascotas"),
    ("perro",             "Mascotas"),
    ("gato",              "Mascotas"),
]


def get_canonical_category(category: str) -> str:
    """
    Mapea la categoría de Consum (en español) a la categoría canónica
    unificada compartida entre todos los supermercados.

    Solo debe recibir categorías con type==0 (reales).
    Ejemplos:
        "Preparados de legumbres y hortalizas" → "Pasta, Arroz y Legumbres"
        "Lácteos y huevos"                    → "Lacteos"
    """
    if not category:
        return "Otros"
    cat_lower = f" {category.lower()} "   # espacios para matching de palabras enteras
    for keyword, canonical in _CANONICAL_RULES:
        if keyword in cat_lower:
            return canonical
    return "Otros"


# ── DB ──────────────────────────────────────────────────────────────────────────────────

def init_db():
    print("📦 Inicializando base de datos...")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id                 TEXT PRIMARY KEY,
            name               TEXT,
            brand              TEXT,
            last_price         REAL,
            unit_size          TEXT,
            category           TEXT,
            image_url          TEXT,
            canonical_category TEXT,
            offer_price        REAL,
            offer_label        TEXT,
            last_update        TIMESTAMP
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

    # Migración segura: añade columnas nuevas en DBs existentes
    _add_column_if_missing(c, "products", "canonical_category", "TEXT")
    _add_column_if_missing(c, "products", "offer_price",        "REAL")
    _add_column_if_missing(c, "products", "offer_label",        "TEXT")

    conn.commit()
    conn.close()
    print("✅ Base de datos lista.")


def _add_column_if_missing(cur: sqlite3.Cursor, table: str, column: str, col_type: str) -> None:
    """ALTER TABLE solo si la columna no existe (SQLite no soporta IF NOT EXISTS)."""
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    if column not in existing:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        print(f"  🔧 Columna '{column}' añadida a '{table}'")


# ── HTTP ────────────────────────────────────────────────────────────────────────────────

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


# ── EXTRACCIÓN DE CAMPOS ────────────────────────────────────────────────────────────────

def extract_product_fields(p: dict) -> dict:
    """
    Extrae todos los campos relevantes del JSON de un producto Consum.

    Estructura de precios confirmada (verificada con la API):
      priceData.prices es una lista de objetos con id + value.centAmount.
      - id == "PRICE"       → precio regular (siempre presente)
      - id == "OFFER_PRICE" → precio de oferta inmediata (solo si hay promoción)
      centAmount viene ya en euros (el nombre del campo es engañoso).

    Estructura de ofertas:
      offers[] → lista de promociones.
      Solo consideramos oferta válida si existe OFFER_PRICE en prices
      (descuento inmediato). Ofertas de pack (sin OFFER_PRICE) se ignoran.

    Categorías:
      categories[] → type==0 son reales; type==1 son promocionales (se ignoran).
    """
    pid       = str(p.get("id", "")).strip()
    prod_data = p.get("productData", {})
    price_data = p.get("priceData", {})

    name      = prod_data.get("name", "Sin nombre").strip()
    brand     = (prod_data.get("brand") or {}).get("name", "").strip()
    image_url = prod_data.get("imageURL", "").strip()
    unit_size = price_data.get("unitPriceUnitType", "").strip()

    # Precios: iterar por id para no depender del orden del array
    regular_price = 0.0
    offer_price   = None
    for price_entry in price_data.get("prices", []):
        amount = price_entry.get("value", {}).get("centAmount", 0)
        if price_entry.get("id") == "PRICE":
            regular_price = float(amount)
        elif price_entry.get("id") == "OFFER_PRICE":
            offer_price = float(amount)

    # Etiqueta de oferta: solo cuando hay descuento inmediato (OFFER_PRICE presente)
    offer_label = None
    if offer_price is not None:
        for offer in p.get("offers") or []:
            label = offer.get("shortDescription", "").strip()
            if label:
                offer_label = label
                break
        if offer_label is None:
            offer_label = "Oferta"

    # Categoría real (type==0); ignorar las promocionales (type==1)
    real_categories = [
        c for c in (p.get("categories") or []) if c.get("type") == 0
    ]
    category           = real_categories[0]["name"].strip() if real_categories else ""
    canonical_category = get_canonical_category(category)

    return {
        "pid":                pid,
        "name":               name,
        "brand":              brand,
        "image_url":          image_url,
        "unit_size":          unit_size,
        "last_price":         regular_price,
        "offer_price":        offer_price,
        "offer_label":        offer_label,
        "category":           category,
        "canonical_category": canonical_category,
    }


# ── PROCESADO DE PRODUCTO ─────────────────────────────────────────────────────────────

def process_product(p: dict) -> None:
    fields = extract_product_fields(p)
    pid    = fields["pid"]
    if not pid:
        return

    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    c.execute("SELECT last_price FROM products WHERE id = ?", (pid,))
    row = c.fetchone()

    new_price = fields["last_price"]

    if row:
        old_price = row[0]
        if old_price != new_price:
            arrow = "▲" if new_price > old_price else "▼"
            print(f"  {arrow} {fields['name']}: {old_price}€ → {new_price}€")
            price_changes.append((pid, fields["name"], old_price, new_price, now))
            c.execute('''
                INSERT INTO price_history (product_id, name, old_price, new_price, change_date)
                VALUES (?, ?, ?, ?, ?)
            ''', (pid, fields["name"], old_price, new_price, now))
        c.execute('''
            UPDATE products
            SET name=?, brand=?, last_price=?, unit_size=?, category=?,
                image_url=?, canonical_category=?, offer_price=?, offer_label=?,
                last_update=?
            WHERE id=?
        ''', (
            fields["name"], fields["brand"], new_price, fields["unit_size"],
            fields["category"], fields["image_url"], fields["canonical_category"],
            fields["offer_price"], fields["offer_label"], now, pid,
        ))
    else:
        print(f"  ✅ Nuevo: {fields['name']} ({new_price}€)")
        c.execute('''
            INSERT INTO products
                (id, name, brand, last_price, unit_size, category, image_url,
                 canonical_category, offer_price, offer_label, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            pid, fields["name"], fields["brand"], new_price, fields["unit_size"],
            fields["category"], fields["image_url"], fields["canonical_category"],
            fields["offer_price"], fields["offer_label"], now,
        ))

    conn.commit()
    conn.close()


# ── FETCH ALL PRODUCTS ──────────────────────────────────────────────────────────────────

def fetch_all_products():
    """
    Pagina /catalog/product usando los parámetros reales de la API:
      page    → número de página (empieza en 1)
      limit   → productos por página (20 fijo por la API)
      offset  → page * limit
      orderById=5 y showProducts=true son obligatorios

    Para cuando:
      - la lista de productos viene vacía
      - hasMore == False
      - total_seen >= totalCount (seguridad anti-bucle)
    """
    LIMIT       = 20   # la API devuelve 20 fijos, no acepta más
    page        = 1
    offset      = 0
    total_seen  = 0
    total_count = None

    while True:
        params = {
            "page":         page,
            "limit":        LIMIT,
            "offset":       offset,
            "orderById":    5,
            "showProducts": "true",
        }
        print(f"\n📄 Página {page} | offset {offset} ({total_seen} procesados)...")

        response = safe_get(f"{BASE_URL}/catalog/product", params=params)
        if response is None:
            print(f"❌ No se pudo obtener la página {page}. Abortando.")
            break

        data     = response.json()
        products = data.get("products", [])

        if total_count is None:
            total_count = data.get("totalCount", 0)
            print(f"📊 Total según API: {total_count} productos (~{-(-total_count // LIMIT)} páginas)")

        if not products:
            print("✅ Lista vacía, no hay más productos.")
            break

        for p in products:
            process_product(p)

        total_seen += len(products)
        print(f"   → {len(products)} procesados | acumulado: {total_seen} / {total_count}")

        if not data.get("hasMore", False):
            print(f"\n✅ hasMore=False. Total final: {total_seen} productos.")
            break

        if total_count and total_seen >= total_count:
            print(f"\n✅ Alcanzado totalCount ({total_count}). Deteniendo.")
            break

        page   += 1
        offset += LIMIT
        time.sleep(SLEEP_REQ)


# ── EXPORT CSV ──────────────────────────────────────────────────────────────────────────

def export_to_csv():
    os.makedirs("data_public", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    with open("data_public/products.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "brand", "last_price", "unit_size",
                    "category", "image_url", "canonical_category",
                    "offer_price", "offer_label", "last_update"])
        for row in c.execute("""
            SELECT id, name, brand, last_price, unit_size,
                   category, image_url, canonical_category,
                   offer_price, offer_label, last_update
            FROM products
            ORDER BY name COLLATE NOCASE
        """):
            w.writerow(row)

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


# ── MAIN ──────────────────────────────────────────────────────────────────────────────

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
