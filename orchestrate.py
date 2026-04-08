from prefect import flow, task, get_run_logger
from google.cloud import storage, bigquery
import os
import subprocess

# CONFIG 
PROJECT_ID = "theta-sunlight-491314-n8"
DATASET = "ecommerce_dw"
BUCKET_NAME = "ecommerce-datalake-gk"
LOCAL_FOLDER = "data"

# Mapping using prefixes so it dynamically catches 2018, 2019, etc.
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

# TASK 1: Upload to Data Lake 
@task(name="Upload to GCS", retries=2, retry_delay_seconds=10)
def upload_files():
    """This function uploads files to GCS"""
    logger = get_run_logger()

    if not os.path.exists(LOCAL_FOLDER):
        logger.warning(f"{LOCAL_FOLDER} not found. Skipping upload step.")
        return "No upload needed"

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    for file_name in os.listdir(LOCAL_FOLDER):
        for prefix, folder in FILE_PREFIX_MAPPING.items():
            if file_name.startswith(prefix) and file_name.endswith(".csv"):
                file_path = os.path.join(LOCAL_FOLDER, file_name)
                blob = bucket.blob(f"raw/{folder}{file_name}")
                blob.upload_from_filename(file_path)
                logger.info(f"Uploaded {file_name} → raw/{folder}")
                break # Move to next file

    return "Upload complete"


# TASK 2: Load to Data Warehouse (WILDCARD EDITION) 
@task(name="Load into BigQuery", retries=2, retry_delay_seconds=10)
def load_all_tables(upstream_trigger):
    logger = get_run_logger()
    client = bigquery.Client(project=PROJECT_ID)
    loaded_tables = []

    # I iterate through the unique folders and use the *.csv wildcard!
    for table_name, folder in zip(
        ["customers", "geolocation", "order_items", "payments", "reviews", "orders", "products", "sellers", "category_translation"],
        ["customer/", "geolocation/", "order_items/", "payments/", "reviews/", "orders/", "products/", "sellers/", "category_translation/"]
    ):
        table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
        uri = f"gs://{BUCKET_NAME}/raw/{folder}*.csv" # <-- THE WILDCARD MAGIC

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            allow_quoted_newlines=True,
            max_bad_records=5000,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

        logger.info(f"Loading {uri} → {table_id} ...")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

        logger.info(f"✅ Loaded {table_name}")
        loaded_tables.append(table_name)

    return loaded_tables


# TASK 3: Transform, Snapshot, and Test.
@task(name="Execute dbt DAG", retries=1, retry_delay_seconds=20)
def run_dbt_dag(loaded_tables):
    logger = get_run_logger()
    logger.info(f"Tables available for dbt: {loaded_tables}")

    # Step 1: SNAPSHOT (Capture history first)
    logger.info("📸 Taking dbt Snapshots...")
    snap_result = subprocess.run(["dbt", "snapshot"], capture_output=True, text=True, cwd="olist_ecommerce")
    logger.info(snap_result.stdout)
    if snap_result.returncode != 0:
        logger.error(snap_result.stderr)
        raise Exception("dbt snapshot failed!")

    # Step 2: RUN (Build the models)
    logger.info("🏗️ Running dbt models...")
    run_result = subprocess.run(["dbt", "run"], capture_output=True, text=True, cwd="olist_ecommerce")
    logger.info(run_result.stdout)
    if run_result.returncode != 0:
        logger.error(run_result.stderr)
        raise Exception("dbt run failed!")
        
    # Step 3: TEST (Check the quality)
    logger.info("🔬 Testing data quality...")
    test_result = subprocess.run(["dbt", "test"], capture_output=True, text=True, cwd="olist_ecommerce")
    logger.info(test_result.stdout) # Print the test results to the terminal
    
    if test_result.returncode != 0:
        logger.error("🚨 DATA QUALITY TEST FAILED! Check the logs.")
        raise Exception("dbt test failed!")

    return "dbt DAG completed successfully"


# THE DAG 
@flow(name="Olist End-to-End Pipeline")
def main_pipeline():
    logger = get_run_logger()
    logger.info("🚀 Starting Olist Pipeline...")

    # The magic of Orchestration: Task 2 waits for Task 1, Task 3 waits for Task 2
    upload_status = upload_files()
    loaded_tables = load_all_tables(upload_status)
    run_dbt_dag(loaded_tables)

    logger.info("🎉 Pipeline Completed Successfully!")


if __name__ == "__main__":
    main_pipeline()