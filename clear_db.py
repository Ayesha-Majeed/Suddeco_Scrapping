import db_utils

def clear_data():
    print("Connecting to database...")
    try:
        # This will delete all existing data from the products table
        db_utils.clear_products_table()
        print("Done! Old product data has been removed.")
        print("Note: The table schema has also been updated to include your new fields.")
    except Exception as e:
        print(f"Error while clearing database: {e}")

if __name__ == "__main__":
    clear_data()
