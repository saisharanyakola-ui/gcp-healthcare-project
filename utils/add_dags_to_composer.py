import argparse
import glob
import os
import tempfile
from shutil import copytree, ignore_patterns
from google.cloud import storage

# =====================================================
# DEFAULT CONFIGURATION (Updated with your values)
# =====================================================

PROJECT_ID = "project-00e61840-f4f1-4e1d-ac8"
COMPOSER_BUCKET = "us-central1-my-airflow-b3e11ed6-bucket"
CLUSTER_NAME = "my-demo-cluster2"

# =====================================================
# PREPARE FILE LIST
# =====================================================

def _create_file_list(directory: str, name_replacement: str):
    """
    Copies relevant files to a temporary folder
    and returns file list.
    """

    if not os.path.exists(directory):
        print(f"⚠️ Directory not found: {directory}")
        return "", []

    temp_dir = tempfile.mkdtemp()

    files_to_ignore = ignore_patterns(
        "__init__.py",
        "*_test.py",
        "__pycache__"
    )

    copytree(
        directory,
        temp_dir,
        ignore=files_to_ignore,
        dirs_exist_ok=True
    )

    files = [
        f for f in glob.glob(
            f"{temp_dir}/**",
            recursive=True
        )
        if os.path.isfile(f)
    ]

    return temp_dir, files

# =====================================================
# UPLOAD TO COMPOSER BUCKET
# =====================================================

def upload_to_composer(
    directory: str,
    bucket_name: str,
    target_folder: str
):
    """
    Upload files to Composer bucket
    """

    temp_dir, files = _create_file_list(
        directory,
        target_folder
    )

    if not files:
        print(f"⚠️ No files found in {directory}")
        return

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)

    for file in files:

        gcs_path = file.replace(
            f"{temp_dir}/",
            target_folder
        )

        try:
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(file)

            print(
                f"✅ Uploaded: {file} "
                f"-> gs://{bucket_name}/{gcs_path}"
            )

        except Exception as e:
            print(f"❌ Failed Upload {file}: {str(e)}")

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Upload DAGs / Data to Composer"
    )

    parser.add_argument(
        "--dags_directory",
        help="Local DAG folder path"
    )

    parser.add_argument(
        "--data_directory",
        help="Local Data folder path"
    )

    parser.add_argument(
        "--bucket",
        default=COMPOSER_BUCKET,
        help="Composer bucket name"
    )

    args = parser.parse_args()

    print("===================================")
    print("PROJECT :", PROJECT_ID)
    print("CLUSTER :", CLUSTER_NAME)
    print("BUCKET  :", args.bucket)
    print("===================================")

    # Upload DAGs
    if args.dags_directory:
        upload_to_composer(
            args.dags_directory,
            args.bucket,
            "dags/"
        )
    else:
        print("⚠️ DAG directory not provided")

    # Upload Data
    if args.data_directory:
        upload_to_composer(
            args.data_directory,
            args.bucket,
            "data/"
        )
    else:
        print("⚠️ Data directory not provided")

    print("✅ Upload Process Completed")