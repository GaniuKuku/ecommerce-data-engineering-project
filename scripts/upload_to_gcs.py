from google.cloud import storage
import os

BUCKET_NAME = "ecommerce-datalake-gk"
LOCAL_FOLDER = "data"

# Mapping files to folders
FILE_MAPPING = {
    "olist_customers_dataset.csv": "customer/",
    "olist_geolocation_dataset.csv": "geolocation/",
    "olist_order_items_dataset.csv": "order_items/",
    "olist_order_payments_dataset.csv": "payments/",
    "olist_order_reviews_dataset.csv": "reviews/",
    "olist_orders_dataset.csv": "orders/",
    "olist_products_dataset.csv": "products/",
    "olist_sellers_dataset.csv": "sellers/",
    "product_category_name_translation.csv": "category_translation/"
}

def upload_files():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name in FILE_MAPPING:
            folder = FILE_MAPPING[file_name]
            file_path = os.path.join(LOCAL_FOLDER, file_name)

            blob = bucket.blob(f"raw/{folder}{file_name}")
            blob.upload_from_filename(file_path)

            print(f"Uploaded {file_name} → raw/{folder}")

if __name__ == "__main__":
    upload_files()