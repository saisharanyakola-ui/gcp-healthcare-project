from datetime import date
import requests
from pyspark.sql import SparkSession

# =====================================================
# CONFIGURATION (Updated with your project details)
# =====================================================

PROJECT_ID = "project-00e61840-f4f1-4e1d-ac8"
BUCKET_NAME = "healthcare-bucket-24"
CLUSTER_NAME = "my-demo-cluster2"
COMPOSER_BUCKET = "us-central1-my-airflow-b3e11ed6-bucket"

OUTPUT_PATH = f"gs://{BUCKET_NAME}/landing/npi_extract/"

# =====================================================
# CURRENT DATE
# =====================================================

current_date = date.today()

# =====================================================
# SPARK SESSION
# =====================================================

spark = SparkSession.builder \
    .appName("NPI_Data_Extraction") \
    .getOrCreate()

# =====================================================
# NPI REGISTRY API
# =====================================================

base_url = "https://npiregistry.cms.hhs.gov/api/"

# Search filters
params = {
    "version": "2.1",
    "state": "CA",
    "city": "Los Angeles",
    "limit": 20
}

# =====================================================
# FETCH NPI LIST
# =====================================================

response = requests.get(base_url, params=params)

if response.status_code == 200:

    npi_data = response.json()
    npi_list = [
        result["number"]
        for result in npi_data.get("results", [])
    ]

    detailed_results = []

    # =================================================
    # FETCH DETAILS FOR EACH NPI
    # =================================================

    for npi in npi_list:

        detail_params = {
            "version": "2.1",
            "number": npi
        }

        detail_response = requests.get(base_url, params=detail_params)

        if detail_response.status_code == 200:

            detail_data = detail_response.json()

            if "results" in detail_data and detail_data["results"]:

                for result in detail_data["results"]:

                    npi_number = result.get("number")
                    basic_info = result.get("basic", {})

                    if result["enumeration_type"] == "NPI-1":
                        first_name = basic_info.get("first_name", "")
                        last_name = basic_info.get("last_name", "")
                    else:
                        first_name = basic_info.get(
                            "authorized_official_first_name", ""
                        )
                        last_name = basic_info.get(
                            "authorized_official_last_name", ""
                        )

                    position = basic_info.get(
                        "authorized_official_title_or_position", ""
                    )

                    organisation = basic_info.get(
                        "organization_name", ""
                    )

                    last_updated = basic_info.get(
                        "last_updated", ""
                    )

                    detailed_results.append({
                        "npi_id": npi_number,
                        "first_name": first_name,
                        "last_name": last_name,
                        "position": position,
                        "organisation_name": organisation,
                        "last_updated": last_updated,
                        "refreshed_at": current_date
                    })

    # =================================================
    # CREATE DATAFRAME & SAVE TO GCS
    # =================================================

    if detailed_results:

        df = spark.createDataFrame(detailed_results)

        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(OUTPUT_PATH)

        print("✅ NPI Data Successfully Saved")
        print(f"📍 Output Path: {OUTPUT_PATH}")
        print(f"📍 Project ID: {PROJECT_ID}")
        print(f"📍 Cluster: {CLUSTER_NAME}")

    else:
        print("No detailed results found.")

else:
    print(f"Failed to fetch data: {response.status_code}")