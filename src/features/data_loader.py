from __future__ import annotations

import os
from typing import Iterator, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sdn_traffic")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sdn_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sdn_password")


class TrafficDataLoader:
    """
    Loads logical traffic events from PostgreSQL.

    One canonical row is selected for each event_id.
    This protects the ML dataset from ingestion/replay duplicates.
    """

    def __init__(self):
        self.engine = self._create_engine()

    @staticmethod
    def _create_engine() -> Engine:
        connection_url = (
            f"postgresql+psycopg2://"
            f"{POSTGRES_USER}:{POSTGRES_PASSWORD}"
            f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )

        return create_engine(
            connection_url,
            pool_pre_ping=True,
        )

    @staticmethod
    def _base_query() -> str:
        return """
            SELECT
                event_id,
                event_timestamp,
                source_node,
                destination_node,
                throughput_mbps
            FROM (
                SELECT
                    id,
                    event_id,
                    event_timestamp,
                    source_node,
                    destination_node,
                    throughput_mbps,
                    ROW_NUMBER() OVER (
                        PARTITION BY event_id
                        ORDER BY id
                    ) AS rn
                FROM traffic_data
            ) t
            WHERE rn = 1
        """

    def fetch_chunks(
        self,
        chunksize: int = 100_000,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Stream the logical traffic dataset in chunks.

        Parameters
        ----------
        chunksize:
            Number of rows loaded into memory at a time.

        start_timestamp:
            Optional inclusive start timestamp.

        end_timestamp:
            Optional exclusive end timestamp.
        """

        query = self._base_query()

        conditions = []
        params = {}

        if start_timestamp:
            conditions.append("event_timestamp >= :start_timestamp")
            params["start_timestamp"] = start_timestamp

        if end_timestamp:
            conditions.append("event_timestamp < :end_timestamp")
            params["end_timestamp"] = end_timestamp

        if conditions:
            query += " AND " + " AND ".join(conditions)

        query += """
            ORDER BY event_timestamp, source_node, destination_node
        """

        with self.engine.connect() as connection:
            for chunk in pd.read_sql_query(
                text(query),
                connection,
                params=params,
                chunksize=chunksize,
            ):
                yield chunk

    def fetch_all(
        self,
        start_timestamp: Optional[str] = None,
        end_timestamp: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch the complete logical dataset.

        For the full 6M-row dataset, prefer fetch_chunks().
        """

        chunks = list(
            self.fetch_chunks(
                chunksize=100_000,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        )

        if not chunks:
            return pd.DataFrame(
                columns=[
                    "event_id",
                    "event_timestamp",
                    "source_node",
                    "destination_node",
                    "throughput_mbps",
                ]
            )

        return pd.concat(chunks, ignore_index=True)


if __name__ == "__main__":
    loader = TrafficDataLoader()

    print("Testing PostgreSQL connection...")

    first_chunk = next(
        loader.fetch_chunks(chunksize=10_000)
    )

    print("\nFirst 10 rows:")
    print(first_chunk.head(10).to_string(index=False))

    print("\nChunk shape:")
    print(first_chunk.shape)

    print("\nColumns:")
    print(first_chunk.columns.tolist())

    print("\nTimestamp range in test chunk:")
    print(first_chunk["event_timestamp"].min())
    print(first_chunk["event_timestamp"].max())

    print("\nUnique event IDs in test chunk:")
    print(first_chunk["event_id"].nunique())