import json
import os
import pathlib
import tempfile
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

CHK_DIR = pathlib.Path(os.getenv("CHECKPOINT_DIR", "./checkpoints_files"))
CHK_DIR.mkdir(exist_ok=True)  # make sure folder exists


def hex_to_int(hex_value: str):
    if hex_value is None:
        return None
    if isinstance(hex_value, int):
        return hex_value
    return int(hex_value, 16) if hex_value.startswith("0x") else int(hex_value, 16)


def _file(name: str) -> pathlib.Path:
    return CHK_DIR / f"{name}.json"


def load_checkpoint(extractor: str) -> int:
    """Return last processed block for <extractor> (0 on first run)."""
    path = _file(extractor)
    if path.exists():
        return json.loads(path.read_text())["last"]
    return 0


def save_checkpoint(extractor: str, block_num: int):
    """Write <extractor>.json atomically."""
    tmp = tempfile.NamedTemporaryFile("w", dir=CHK_DIR, delete=False)
    json.dump({"last": block_num}, tmp.file)
    tmp.flush()
    os.fsync(tmp.fileno())
    tmp.close()
    os.replace(tmp.name, _file(extractor))


def get_partition_path(base_dir, data_type, first_block_num, last_block_num):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(
        base_dir,
        data_type,
        f"date={date_str}",
        f"block_range={first_block_num}_{last_block_num}",
    )
