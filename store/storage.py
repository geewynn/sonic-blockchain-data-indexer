import polars as pl
import os
from dotenv import load_dotenv
import boto3

load_dotenv()

STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")
STORAGE_ENDPOINT_URL = os.getenv("STORAGE_ENDPOINT_URL")
STORAGE_BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    endpoint_url=STORAGE_ENDPOINT_URL,
    aws_access_key_id=STORAGE_ACCESS_KEY,
    aws_secret_access_key=STORAGE_SECRET_KEY,
)


def upload_to_storage(file_path, object_key=None):
    """Upload a file to Hetzner Cloud Storage."""
    if object_key is None:
        object_key = os.path.basename(file_path)

    try:
        s3_client.upload_file(file_path, STORAGE_BUCKET_NAME, object_key)
        print(f"Uploaded to Hetzner: s3://{STORAGE_BUCKET_NAME}/{object_key}")

        # Optionally remove the local file after successful upload
        os.remove(file_path)
        print(f"Removed local file: {file_path}")

        return f"s3://{STORAGE_BUCKET_NAME}/{object_key}"
    except Exception as e:
        print(f"Error uploading to Hetzner: {str(e)}")
        return None
