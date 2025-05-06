import logging
import os

import boto3

from utils.config import (STORAGE_ACCESS_KEY, STORAGE_BUCKET_NAME,
                          STORAGE_ENDPOINT_URL, STORAGE_SECRET_KEY)
from utils.logger_settings import get_logger

logger = get_logger("storage")

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
        logging.info(f"Uploaded to Hetzner: s3://{STORAGE_BUCKET_NAME}/{object_key}")

        # Optionally remove the local file after successful upload
        os.remove(file_path)
        logging.info(f"Removed local file: {file_path}")

        return f"s3://{STORAGE_BUCKET_NAME}/{object_key}"
    except Exception as e:
        logging.info(f"Error uploading to Hetzner: {str(e)}")
        return None
