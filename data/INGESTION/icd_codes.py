import requests
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DateType, BooleanType
)

# =====================================================
# CONFIGURATION (Updated with your project details)
# =====================================================

PROJECT_ID = "project-00e61840-f4f1-4e1d-ac8"
BUCKET_NAME = "healthcare-bucket-24"
CLUSTER_NAME = "my-demo-cluster2"
COMPOSER_BUCKET = "us-central1-my-airflow-b3e11ed6-bucket"

# WHO ICD API Credentials
CLIENT_ID = "09bf0f21-9dc3-41e0-966e-8ae3d476cc42_17a6ae0c-9a45-422d-a28b-796746818192"
CLIENT_SECRET = "LygaivVEeV6GFKSgXOePgC7fB2eAf0aIxR2pqgtsPAQ="
TOKEN_ENDPOINT = "https://icdaccessmanagement.who.int/connect/token"

API_VERSION = "v2"
ACCEPT_LANGUAGE = "en"

# Root ICD Category
ROOT_URL = "https://id.who.int/icd/release/10/2019/A00-A09"

# Output Path
OUTPUT_PATH = f"gs://{BUCKET_NAME}/landing/icd_codes/"

# =====================================================
# CREATE SPARK SESSION
# =====================================================

spark = SparkSession.builder \
    .appName("ICD_Codes_Extraction") \
    .getOrCreate()

# =====================================================
# GET ACCESS TOKEN
# =====================================================

def get_access_token():
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "icdapi_access",
        "grant_type": "client_credentials"
    }

    response = requests.post(TOKEN_ENDPOINT, data=payload, verify=False)

    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(
            f"Token Error: {response.status_code} - {response.text}"
        )

# =====================================================
# FETCH ICD DATA
# =====================================================

def fetch_icd_data(url, headers):
    response = requests.get(url, headers=headers, verify=True)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"API Error: {response.status_code} - {response.text}"
        )

# =====================================================
# RECURSIVE EXTRACTION
# =====================================================

def extract_codes(url, headers):
    data = fetch_icd_data(url, headers)
    codes = []

    if "child" in data:
        for child_url in data["child"]:
            codes.extend(extract_codes(child_url, headers))
    else:
        if "code" in data and "title" in data:
            codes.append({
                "icd_code": data["code"],
                "icd_code_type": "ICD-10",
                "code_description": data["title"]["@value"],
                "inserted_date": datetime.now().date(),
                "updated_date": datetime.now().date(),
                "is_current_flag": True
            })

    return codes

# =====================================================
# MAIN PROCESS
# =====================================================

# Step 1: Token
token = get_access_token()

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/json",
    "Accept-Language": ACCEPT_LANGUAGE,
    "API-Version": API_VERSION
}

# Step 2: Extract Codes
icd_codes = extract_codes(ROOT_URL, headers)

# =====================================================
# SCHEMA
# =====================================================

schema = StructType([
    StructField("icd_code", StringType(), True),
    StructField("icd_code_type", StringType(), True),
    StructField("code_description", StringType(), True),
    StructField("inserted_date", DateType(), True),
    StructField("updated_date", DateType(), True),
    StructField("is_current_flag", BooleanType(), True)
])

# =====================================================
# CREATE DATAFRAME
# =====================================================

df = spark.createDataFrame(icd_codes, schema=schema)

# =====================================================
# SAVE TO GCS
# =====================================================

df.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("✅ ICD Codes Successfully Saved")
print(f"📍 Output Path: {OUTPUT_PATH}")
print(f"📍 Project: {PROJECT_ID}")
print(f"📍 Cluster: {CLUSTER_NAME}")