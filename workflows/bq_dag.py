import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =====================================================
# PROJECT CONFIGURATION (Updated)
# =====================================================

PROJECT_ID = "project-00e61840-f4f1-4e1d-ac8"
REGION = "us-central1"
LOCATION = "US"

COMPOSER_BUCKET = "us-central1-my-airflow-b3e11ed6-bucket"
CLUSTER_NAME = "my-demo-cluster2"

# SQL FILE PATHS (inside Composer bucket mounted path)
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"

# =====================================================
# READ SQL FILES
# =====================================================

def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# =====================================================
# DEFAULT ARGUMENTS
# =====================================================

ARGS = {
    "owner": "SHAIK SAIDHUL",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# =====================================================
# DAG DEFINITION
# =====================================================

with DAG(
    dag_id="bigquery_etl_pipeline",
    default_args=ARGS,
    description="Bronze to Silver to Gold BigQuery ETL Pipeline",
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "composer", "etl", "healthcare"]
) as dag:

    # -------------------------------------------------
    # Bronze Layer
    # -------------------------------------------------

    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location=LOCATION
    )

    # -------------------------------------------------
    # Silver Layer
    # -------------------------------------------------

    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location=LOCATION
    )

    # -------------------------------------------------
    # Gold Layer
    # -------------------------------------------------

    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
        location=LOCATION
    )

    # =================================================
    # PIPELINE FLOW
    # =================================================

    bronze_tables >> silver_tables >> gold_tables


    #--gggg