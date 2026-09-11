from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.features.data_loader import TrafficDataLoader


OUTPUT_PATH = Path("data/ml/traffic_clean.parquet")
CHUNK_SIZE = 100_000


def export_dataset():
    loader = TrafficDataLoader()

    OUTPUT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    writer = None
    total_rows = 0
    chunk_number = 0

    try:
        for chunk in loader.fetch_chunks(
            chunksize=CHUNK_SIZE
        ):
            chunk_number += 1

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False,
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    OUTPUT_PATH,
                    table.schema,
                    compression="snappy",
                )

            writer.write_table(table)

            total_rows += len(chunk)

            print(
                f"Chunk {chunk_number:03d} | "
                f"rows={len(chunk):,} | "
                f"total={total_rows:,}"
            )

    finally:
        if writer is not None:
            writer.close()

    print("\nExport completed.")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Total rows: {total_rows:,}")


if __name__ == "__main__":
    export_dataset()