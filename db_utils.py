import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def create_table():
    conn = get_connection()
    cur = conn.cursor()
    # Create table with all supported columns
    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE,
            name TEXT,
            sku TEXT,
            brand TEXT,
            price NUMERIC,
            region TEXT,
            supplier TEXT,
            select_task TEXT,
            all_images TEXT,
            material_unit TEXT,
            description TEXT,
            quantity TEXT,
            pieces_in_pack TEXT,
            coverage_m2 TEXT,
            volume_m3 TEXT,
            product_length_m TEXT,
            product_width TEXT,
            product_thickness TEXT,
            product_weight_kg TEXT,
            product_type TEXT,
            material TEXT
        );
    """)
    
    # Add columns if they don't exist (handle evolution for existing tables)
    columns = [
        ("description", "TEXT"),
        ("quantity", "TEXT"),
        ("pieces_in_pack", "TEXT"),
        ("coverage_m2", "TEXT"),
        ("volume_m3", "TEXT"),
        ("product_length_m", "TEXT"),
        ("product_width", "TEXT"),
        ("product_thickness", "TEXT"),
        ("product_weight_kg", "TEXT"),
        ("product_type", "TEXT"),
        ("material", "TEXT")
    ]
    for col_name, col_type in columns:
        try:
            cur.execute(f"ALTER TABLE products ADD COLUMN IF NOT EXISTS {col_name} {col_type};")
        except Exception as e:
            print(f"Note: Could not add column {col_name}: {e}")
            
    conn.commit()
    cur.close()
    conn.close()

def product_exists(url):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM products WHERE url = %s;", (url,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists

def save_product(product):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO products (
                url, name, sku, brand, price, region, supplier, select_task, all_images, material_unit,
                description, quantity, pieces_in_pack, coverage_m2, volume_m3, 
                product_length_m, product_width, product_thickness, product_weight_kg, 
                product_type, material
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                name = EXCLUDED.name,
                sku = EXCLUDED.sku,
                brand = EXCLUDED.brand,
                price = EXCLUDED.price,
                region = EXCLUDED.region,
                supplier = EXCLUDED.supplier,
                select_task = EXCLUDED.select_task,
                all_images = EXCLUDED.all_images,
                material_unit = EXCLUDED.material_unit,
                description = EXCLUDED.description,
                quantity = EXCLUDED.quantity,
                pieces_in_pack = EXCLUDED.pieces_in_pack,
                coverage_m2 = EXCLUDED.coverage_m2,
                volume_m3 = EXCLUDED.volume_m3,
                product_length_m = EXCLUDED.product_length_m,
                product_width = EXCLUDED.product_width,
                product_thickness = EXCLUDED.product_thickness,
                product_weight_kg = EXCLUDED.product_weight_kg,
                product_type = EXCLUDED.product_type,
                material = EXCLUDED.material;
        """, (
            product.get("Link"),
            product.get("Name"),
            product.get("SKU"),
            product.get("Brand"),
            product.get("Price_Inc_VAT"),
            product.get("Region"),
            product.get("Supplier"),
            product.get("Select Task"),
            product.get("All_Images"),
            product.get("Material Unit"),
            product.get("description"),
            product.get("Quantity"),
            product.get("Pieces_in_Pack"),
            product.get("Coverage_M2"),
            product.get("Volume_M3"),
            product.get("Product_Length_M"),
            product.get("Product_Width"),
            product.get("Product_Thickness"),
            product.get("Product_Weight_Kg"),
            product.get("Product_Type"),
            product.get("Material"),
        ))
        conn.commit()
    except Exception as e:
        print(f"Error saving product: {e}")
    finally:
        cur.close()
        conn.close()

def create_products_db():
    # Connect to default database to create the "Products" DB
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='ashu123', # Maintain user's password from original
        host='localhost',
        port='5432'
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute('CREATE DATABASE "Products";')
    except:
        pass # Already exists
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        create_products_db()
    except Exception as e:
        print(f"Info: {e}")
    create_table()