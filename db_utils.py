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
            material_unit TEXT
        );
    """)
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
            INSERT INTO products (url, name, sku, brand, price, region, supplier, select_task, all_images, material_unit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
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
        ))
        conn.commit()
    except Exception as e:
        print(f"Error saving product: {e}")
    finally:
        cur.close()


def create_products_db():
    conn = psycopg2.connect(
        dbname='postgres',      # Connect to default database
        user='postgres',        # Use your actual username
        password='ashu123',     # Use your actual password
        host='localhost',
        port='5432'
    )
    conn.autocommit = True  # <-- Add this line!
    cur = conn.cursor()
    cur.execute('CREATE DATABASE "Products";')
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        create_products_db()
    except Exception as e:
        print(f"Info: {e}")
    create_table()