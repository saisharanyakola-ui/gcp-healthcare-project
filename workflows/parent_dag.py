import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# =====================================================
# PROJECT CONFIGURATION
# =====================================================

PROJECT_ID = "project-00e61840-f4f1-4e1d-ac8"
REGION = "us-central1"
CLUSTER_NAME = "my-demo-cluster2"
COMPOSER_BUCKET = "us-central1-my-airflow-b3e11ed6-bucket"

# =====================================================
# DEFAULT ARGUMENTS
# =====================================================

ARGS = {
    "owner": "SHAIK SAIDHUL",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# =====================================================
# PARENT DAG
# =====================================================

with DAG(
    dag_id="parent_etl_dag",
    schedule_interval="0 5 * * *",   # Daily at 5 AM
    catchup=False,
    default_args=ARGS,
    description="Master DAG to trigger PySpark then BigQuery pipeline",
    tags=["parent", "composer", "etl", "healthcare"]
) as dag:

    # -------------------------------------------------
    # Trigger PySpark DAG
    # -------------------------------------------------

    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
        poke_interval=60
    )

    # -------------------------------------------------
    # Trigger BigQuery DAG
    # -------------------------------------------------

    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_etl_pipeline",
        wait_for_completion=True,
        poke_interval=60
    )

    # =================================================
    # EXECUTION FLOW
    # =================================================

    trigger_pyspark_dag >> trigger_bigquery_dag