from google.cloud import bigquery

# -------- CONFIG --------
PROJECT_ID = "theta-sunlight-491314-n8"  
DATASET = "ecommerce_dw"
BUCKET = "ecommerce-datalake-gk"

# Mapping using the wildcard (*.csv) to grab ALL files in the folder
TABLES = {
    "orders": "raw/orders/*.csv",
    "customers": "raw/customer/*.csv",
    "order_items": "raw/order_items/*.csv",
    "payments": "raw/payments/*.csv",
    "reviews": "raw/reviews/*.csv",
    "products": "raw/products/*.csv",
    "sellers": "raw/sellers/*.csv",
    "geolocation": "raw/geolocation/*.csv",
    "category_translation": "raw/category_translation/*.csv"
}

# -------- MAIN FUNCTION --------
def load_all_tables():
    client = bigquery.Client(project=PROJECT_ID)

    for table_name, gcs_path in TABLES.items():
        table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
        uri = f"gs://{BUCKET}/{gcs_path}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            allow_quoted_newlines=True,
            max_bad_records=5000,
            autodetect=True,              # <-- auto-detect schema
            write_disposition="WRITE_TRUNCATE"  # replace table if it exists
        )

        print(f"Loading {uri} → {table_id} ...")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # wait until finished
        print(f"✅ Loaded {table_name}")

if __name__ == "__main__":
    load_all_tables()