import polars as pl
import os
from pathlib import Path


def write_batch_to_parquet(data_list, data_type, batch_id, output_dir):
    """Write a batch of data to a parquet file."""
    if not data_list:
        return
    
    df = pl.DataFrame([data for data in data_list if data is not None])
    subdir = os.path.join(output_dir, f"{data_type}")
    Path(subdir).mkdir(parents=True, exist_ok=True)

    output_path = os.path.join(subdir, f"{data_type}_batch_{batch_id}.parquet")
    df.write_parquet(output_path, compression='snappy')

    print(f"Wrote {len(data_list)} {data_type} to {output_path}")
    
    return output_path