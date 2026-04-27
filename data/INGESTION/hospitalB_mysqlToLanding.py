from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Initialize Spark Session
spark = SparkSession.builder.appName("HospitalBMySQLToLanding").getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "healthcare-bucket-24"
HOSPITAL_NAME = "hospital-b"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

# BigQuery Configuration
BQ_PROJECT = "project-00e61840-f4f1-4e1d-ac8"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = "us-central1-my-airflow-b3e11ed6-bucket/temp/"

# MySQL Configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.57.108.23/hospital_b_db?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Temp123!"
}

# ------------------------------------------------------------------------------------------------------------------#
# Logging Mechanism
log_entries = []

def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs():
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json.dumps(log_entries, indent=4), content_type="application/json")

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        (log_df.write.format("bigquery")
            .option("table", BQ_LOG_TABLE)
            .option("temporaryGcsBucket", BQ_TEMP_PATH)
            .mode("append")
            .save())

# ------------------------------------------------------------------------------------------------------------------#
# Archive Existing Files
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        storage_client.bucket(GCS_BUCKET).copy_blob(
            source_blob,
            storage_client.bucket(GCS_BUCKET),
            destination_blob.name
        )
        source_blob.delete()

# ------------------------------------------------------------------------------------------------------------------#
# Get Latest Watermark
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}'
          AND data_source = 'hospital_b_db'
    """

    result = bq_client.query(query).result()

    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"

    return "1900-01-01 00:00:00"

# ------------------------------------------------------------------------------------------------------------------#
# Extract and Save
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None

        query = (
            f"(SELECT * FROM {table}) AS t"
            if load_type.lower() == "full"
            else f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        )

        df = (spark.read.format("jdbc")
            .option("url", MYSQL_CONFIG["url"])
            .option("user", MYSQL_CONFIG["user"])
            .option("password", MYSQL_CONFIG["password"])
            .option("driver", MYSQL_CONFIG["driver"])
            .option("dbtable", query)
            .load())

        today = datetime.datetime.today().strftime('%d%m%Y')
        json_file = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

        storage_client.bucket(GCS_BUCKET).blob(json_file).upload_from_string(
            df.toPandas().to_json(orient="records", lines=True),
            content_type="application/json"
        )

        audit_df = spark.createDataFrame(
            [("hospital_b_db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")],
            ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"]
        )

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", BQ_TEMP_PATH)
            .mode("append")
            .save())

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)

# ------------------------------------------------------------------------------------------------------------------#
# Read Config
def read_config_file():
    return spark.read.csv(CONFIG_FILE_PATH, header=True)

config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == "1" and row["datasource"] == "hospital_b_db":
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()