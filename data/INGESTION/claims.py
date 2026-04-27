from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# Create Spark session
spark = SparkSession.builder \
                    .appName("Healthcare Claims Ingestion") \
                    .getOrCreate()

# configure variables
BUCKET_NAME = "healthcare-bucket-24"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "project-00e61840-f4f1-4e1d-ac8.bronze_dataset.claims"
TEMP_GCS_BUCKET = "us-central1-my-airflow-b3e11ed6-bucket/temp/"

# read from claims source
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# adding hospital source for future reference
claims_df = (
    claims_df.withColumn(
        "datasource",
        when(input_file_name().contains("hospital2"), "hosb")
        .when(input_file_name().contains("hospital1"), "hosa")
        .otherwise("None")
    )
)

# dropping duplicates if any
claims_df = claims_df.dropDuplicates()

# write to bigquery
(claims_df.write
    .format("bigquery")
    .option("table", BQ_TABLE)
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
    .mode("overwrite")
    .save())