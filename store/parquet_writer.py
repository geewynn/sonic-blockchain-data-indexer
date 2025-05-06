import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from utils.logger_settings import get_logger

logger = get_logger("parquet_writer")


def write_batch_to_parquet(data_list, data_type, batch_id, output_dir):
    """Write a batch of data to a parquet file using PyArrow directly."""
    if not data_list or all(item is None for item in data_list):
        return

    # Filter out None items
    data_list = [item for item in data_list if item is not None]

    table = pa.Table.from_pylist(data_list)
    # Create output directory
    subdir = os.path.join(output_dir, f"{data_type}")
    Path(subdir).mkdir(parents=True, exist_ok=True)

    # Define output path
    output_path = os.path.join(subdir, f"{data_type}_batch_{batch_id}.parquet")

    # Write using PyArrow with full control over options
    pq.write_table(
        table,
        output_path,
        compression="snappy",
        use_dictionary=True,
        version="2.6",  # Latest Parquet version for maximum compatibility
        use_deprecated_int96_timestamps=False,
    )

    logger.info(f"Wrote {len(data_list)} {data_type} to {output_path}")

    return output_path
