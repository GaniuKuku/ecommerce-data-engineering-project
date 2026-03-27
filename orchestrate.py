from prefect import flow, task, get_run_logger
from google.cloud import storage, bigquery
import os
import subprocess

# -------- CONFIG --------
PROJECT_ID = "theta-sunlight-491314-n8"
DATASET = "ecommerce_dw"
BUCKET_NAME = "ecommerce-datalake-gk"
LOCAL_FOLDER = "data"

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

# --- TASK 1: Upload to Data Lake ---
@task(name="Upload to GCS", retries=2, retry_delay_seconds=10)
def upload_files():
    logger = get_run_logger()

    if not os.path.exists(LOCAL_FOLDER):
        raise FileNotFoundError(f"{LOCAL_FOLDER} not found")

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    uploaded_files = []

    for file_name in os.listdir(LOCAL_FOLDER):
        if file_name in FILE_MAPPING:
            folder = FILE_MAPPING[file_name]
            file_path = os.path.join(LOCAL_FOLDER, file_name)

            blob = bucket.blob(f"raw/{folder}{file_name}")
            blob.upload_from_filename(file_path)

            logger.info(f"Uploaded {file_name} → raw/{folder}")
            uploaded_files.append((file_name, folder))

    return uploaded_files


# --- TASK 2: Load to Data Warehouse ---
@task(name="Load into BigQuery", retries=2, retry_delay_seconds=10)
def load_all_tables(uploaded_files):
    logger = get_run_logger()

    client = bigquery.Client(project=PROJECT_ID)

    loaded_tables = []

    for file_name, folder in uploaded_files:
        table_name = folder.replace("/", "")
        table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
        uri = f"gs://{BUCKET_NAME}/raw/{folder}{file_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            allow_quoted_newlines=True,
            max_bad_records=5000,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

        logger.info(f"Loaded {table_name}")
        loaded_tables.append(table_name)

    return loaded_tables


# --- TASK 3: Transform Data ---
@task(name="Run dbt Models", retries=2, retry_delay_seconds=20)
def run_dbt(loaded_tables):
    logger = get_run_logger()
    logger.info(f"Tables available for dbt: {loaded_tables}")

    # Add the 'cwd' argument to point to your dbt folder!
    # Change "olist_ecommerce" to whatever your dbt folder is actually named (e.g. "dbt_project")
    result = subprocess.run(
        ["dbt", "run"], 
        capture_output=True, 
        text=True,
        cwd="olist_ecommerce" # <--- THIS IS THE FIX
    )

    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise Exception("dbt run failed!")

    return "dbt completed"


# --- THE DAG ---
@flow(name="Olist End-to-End Pipeline")
def main_pipeline():
    logger = get_run_logger()
    logger.info("🚀 Starting Olist Pipeline...")

    uploaded_files = upload_files()
    loaded_tables = load_all_tables(uploaded_files)
    run_dbt(loaded_tables)

    logger.info("🎉 Pipeline Completed Successfully!")


if __name__ == "__main__":
    main_pipeline()