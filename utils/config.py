import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _need(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing env var: {name}")
    return val


RPC_URL: str = _need("RPC_URL")
OUTPUT_DIR: Path = Path(_need("OUTPUT_DIR"))
END_BLOCK: int = int(_need("END_BLOCK"))

LOGS_FOLDER: str = _need("LOGS_FOLDER")
TRACES_FOLDER: str = _need("TRACES_FOLDER")
BLOCKS_FOLDER: str = _need("BLOCKS_FOLDER")
RECEIPT_FOLDER: str = _need("RECEIPT_FOLDER")

BATCH_BLOCKS: int = int(os.getenv("BATCH_BLOCKS", 5_000))
MICRO_BATCH: int = int(os.getenv("MICRO_BATCH_SIZE", 20))

# logging
LOG_DIR: Path = Path(os.getenv("LOG_DIR", "./logs"))
LOG_FILE: str = os.getenv("LOG_FILE", "crawler.log")
LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", 50_000_000))
LOG_BACKUPS: int = int(os.getenv("LOG_BACKUPS", 2))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()


STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY")
STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY")
STORAGE_ENDPOINT_URL = os.getenv("STORAGE_ENDPOINT_URL")
STORAGE_BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME")
