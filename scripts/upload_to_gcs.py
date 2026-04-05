from google.cloud import storage
import os

BUCKET_NAME = "ecommerce-datalake-gk"
# Look exactly where the files are hiding
LOCAL_FOLDER = "data/new_batch"

FILE_PREFIX_MAPPING = {
    "olist_customers_dataset": "customer/",
    "olist_geolocation_dataset": "geolocation/",
    "olist_order_items_dataset": "order_items/",
    "olist_order_payments_dataset": "payments/",
    "olist_order_reviews_dataset": "reviews/",
    "olist_orders_dataset": "orders/",
    "olist_products_dataset": "products/",
    "olist_sellers_dataset": "sellers/",
    "product_category_name_translation": "category_translation/"
}

def upload_files():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    if not os.path.exists(LOCAL_FOLDER):
        print(f"Error: Could not find '{LOCAL_FOLDER}'")
        return

    for file_name in os.listdir(LOCAL_FOLDER):
        for prefix, folder in FILE_PREFIX_MAPPING.items():
            if file_name.startswith(prefix) and file_name.endswith(".csv"):
                file_path = os.path.join(LOCAL_FOLDER, file_name)

                # Uploads exactly what you named them: raw/orders/olist_orders_dataset_2019.csv
                blob = bucket.blob(f"raw/{folder}{file_name}")
                blob.upload_from_filename(file_path)

                print(f"Uploaded {file_name} → raw/{folder}")
                break

if __name__ == "__main__":
    upload_files()