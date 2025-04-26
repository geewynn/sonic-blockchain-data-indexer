import os
from pathlib import Path

def setup_directory(output_dir):
    for subdir in ["blocks_test", "receipts_test", "logs_test", "traces_test"]:
        path = os.path.join(output_dir, subdir)
        Path(path).mkdir(parents=True, exist_ok=True)