import os
from pathlib import Path

def setup_directory(output_dir):
    for subdir in ["blocks", "receipts", "logs_data", "traces_data"]:
        path = os.path.join(output_dir, subdir)
        Path(path).mkdir(parents=True, exist_ok=True)