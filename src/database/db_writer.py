import json
import logging
import os
import signal
import sys
import time

import psycopg
from kafka import KafkaConsumer


# =========================================================
# Configuration
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic.validated")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "postgres-db-writer")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sdn_traffic")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sdn_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sdn_password")

# PostgreSQL transaction batch size.
# 100K is recommended for the historical 6M+ load.
BATCH_SIZE = int(os.getenv("DB_BATCH_SIZE", "20000"))

# Kafka poll size. Keep this below BATCH_SIZE so the consumer
# remains responsive while the application accumulates a DB batch.
KAFKA_POLL_RECORDS = int(os.getenv("KAFKA_POLL_RECORDS", "5000"))

# Time-based flush is only a safety valve for sparse traffic.
FLUSH_INTERVAL_SECONDS = int(os.getenv("DB_FLUSH_INTERVAL_SECONDS", "30"))

# Kafka consumer timing.
MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "600000"))
SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000"))
HEARTBEAT_INTERVAL_MS = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "15000"))
REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "60000"))


# =========================================================
# Logging
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("db_writer")


# =========================================================
# Graceful shutdown
# =========================================================

running = True


def shutdown_handler(signum, frame):
    global running

    logger.info(
        "Shutdown signal received. Finishing current batch..."
    )
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# =========================================================
# PostgreSQL connection
# =========================================================

def create_db_connection():
    connection_string = (
        f"host={POSTGRES_HOST} "
        f"port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    connection = psycopg.connect(connection_string)
    connection.autocommit = False
    return connection


# =========================================================
# PostgreSQL staging table
# =========================================================

CREATE_STAGING_TABLE = """
CREATE TEMP TABLE IF NOT EXISTS traffic_data_staging
(LIKE traffic_data INCLUDING DEFAULTS)
ON COMMIT DELETE ROWS
"""

TRUNCATE_STAGING_TABLE = """
TRUNCATE traffic_data_staging
"""

COPY_SQL = """
COPY traffic_data_staging (
    event_id,
    event_timestamp,
    source_node,
    destination_node,
    source_ip,
    destination_ip,
    protocol,
    packet_count,
    byte_count,
    duration_ms,
    throughput_mbps,
    packet_loss_percent,
    latency_ms,
    raw_payload,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
FROM STDIN
"""

INSERT_FROM_STAGING_SQL = """
INSERT INTO traffic_data (
    event_id,
    event_timestamp,
    source_node,
    destination_node,
    source_ip,
    destination_ip,
    protocol,
    packet_count,
    byte_count,
    duration_ms,
    throughput_mbps,
    packet_loss_percent,
    latency_ms,
    raw_payload,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
SELECT
    event_id,
    event_timestamp,
    source_node,
    destination_node,
    source_ip,
    destination_ip,
    protocol,
    packet_count,
    byte_count,
    duration_ms,
    throughput_mbps,
    packet_loss_percent,
    latency_ms,
    raw_payload,
    kafka_topic,
    kafka_partition,
    kafka_offset
FROM traffic_data_staging
ON CONFLICT (
    kafka_topic,
    kafka_partition,
    kafka_offset
)
DO NOTHING
"""


def prepare_staging_table(connection):
    """Create the temporary staging table once per DB connection."""
    with connection.cursor() as cursor:
        cursor.execute(CREATE_STAGING_TABLE)
    connection.commit()
    logger.info("PostgreSQL COPY staging table ready")


# =========================================================
# Generic value extraction
# =========================================================

def get_value(payload, *keys):
    """Return the first available value from the payload."""
    for key in keys:
        if key in payload:
            return payload[key]
    return None


# =========================================================
# Timestamp conversion
# =========================================================

def convert_timestamp(value):
    """Return the ISO timestamp for psycopg/PostgreSQL."""
    return value


# =========================================================
# Kafka message -> PostgreSQL row
# =========================================================

def message_to_row(message):
    """Convert one Kafka JSON message into a PostgreSQL COPY row."""

    payload = message.value

    if not isinstance(payload, dict):
        raise ValueError(
            f"Expected JSON object but received {type(payload).__name__}"
        )

    event_id = get_value(payload, "event_id", "eventId", "id")
    if event_id is None:
        raise ValueError("Validated message does not contain event_id")

    event_timestamp = convert_timestamp(
        get_value(
            payload,
            "event_timestamp",
            "timestamp",
            "event_time",
            "eventTime"
        )
    )

    source_node = get_value(
        payload,
        "source_node",
        "sourceNode",
        "src_node"
    )

    destination_node = get_value(
        payload,
        "destination_node",
        "destinationNode",
        "dst_node"
    )

    source_ip = get_value(payload, "source_ip", "sourceIp", "src_ip")
    destination_ip = get_value(
        payload,
        "destination_ip",
        "destinationIp",
        "dst_ip"
    )
    protocol = get_value(payload, "protocol")

    packet_count = get_value(payload, "packet_count", "packetCount")
    byte_count = get_value(payload, "byte_count", "byteCount")
    duration_ms = get_value(payload, "duration_ms", "durationMs")

    throughput_mbps = get_value(
        payload,
        "throughput_mbps",
        "throughputMbps",
        "traffic_mbps"
    )

    packet_loss_percent = get_value(
        payload,
        "packet_loss_percent",
        "packetLossPercent"
    )

    latency_ms = get_value(payload, "latency_ms", "latencyMs")

    # Keep JSON as text. PostgreSQL casts it to JSONB when inserting
    # into the jsonb column, as in the existing writer.
    raw_payload = json.dumps(payload, ensure_ascii=False)

    return (
        str(event_id),
        event_timestamp,
        source_node,
        destination_node,
        source_ip,
        destination_ip,
        protocol,
        packet_count,
        byte_count,
        duration_ms,
        throughput_mbps,
        packet_loss_percent,
        latency_ms,
        raw_payload,
        message.topic,
        message.partition,
        message.offset
    )


# =========================================================
# Batch insertion using PostgreSQL COPY
# =========================================================

def insert_batch(connection, batch):
    """
    Bulk insert a complete batch using PostgreSQL COPY.

    PostgreSQL is committed here. Kafka offsets are committed by
    the caller only after this function returns successfully.

    The production insert retains the existing Kafka-offset
    uniqueness guarantee with ON CONFLICT DO NOTHING.
    """

    if not batch:
        return 0

    batch_size = len(batch)
    started = time.perf_counter()

    logger.info(
        "COPY inserting batch of %d records into PostgreSQL...",
        batch_size
    )

    try:
        with connection.cursor() as cursor:
            # The previous transaction committed with
            # ON COMMIT DELETE ROWS, so this is normally already empty.
            # TRUNCATE also protects against future transaction changes.
            cursor.execute(TRUNCATE_STAGING_TABLE)

            # Fast bulk transfer into the temporary staging table.
            with cursor.copy(COPY_SQL) as copy:
                for row in batch:
                    copy.write_row(row)

            # Idempotent insert into the real table.
            cursor.execute(INSERT_FROM_STAGING_SQL)
            inserted = cursor.rowcount

        # CRITICAL: PostgreSQL COMMIT happens BEFORE Kafka COMMIT.
        connection.commit()

        elapsed = time.perf_counter() - started
        rate = batch_size / elapsed if elapsed > 0 else 0.0
        duplicates = batch_size - inserted

        logger.info(
            "DB BATCH COMMITTED | received=%d | inserted=%d | "
            "duplicates=%d | batch_time=%.3fs | batch_rate=%.2f records/sec",
            batch_size,
            inserted,
            duplicates,
            elapsed,
            rate
        )

        return inserted

    except Exception:
        connection.rollback()
        logger.exception(
            "Database COPY failed. Transaction rolled back. "
            "Kafka offsets will NOT be committed."
        )
        raise


# =========================================================
# Kafka consumer
# =========================================================

def create_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,

        # Manual offset management.
        enable_auto_commit=False,
        auto_offset_reset="earliest",

        # Poll tuning.
        max_poll_records=KAFKA_POLL_RECORDS,
        max_poll_interval_ms=MAX_POLL_INTERVAL_MS,

        # Coordinator / heartbeat / request timing.
        session_timeout_ms=SESSION_TIMEOUT_MS,
        heartbeat_interval_ms=HEARTBEAT_INTERVAL_MS,
        request_timeout_ms=REQUEST_TIMEOUT_MS,

        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        )
    )

    return consumer


# =========================================================
# Kafka offset commit
# =========================================================

def commit_batch_offsets(consumer, batch):
    """
    Commit exactly the offsets belonging to the successfully
    persisted batch. Kafka offsets are message offset + 1.

    Using explicit offsets is safer than consumer.commit() because
    it cannot accidentally commit records outside the DB batch.
    """

    offsets = {}

    for message in batch:
        tp = (message.topic, message.partition)
        next_offset = message.offset + 1

        # Keep the highest offset + 1 for each partition.
        current = offsets.get(tp)
        if current is None or next_offset > current:
            offsets[tp] = next_offset

    kafka_offsets = {}

    from kafka.structs import TopicPartition, OffsetAndMetadata

    for (topic, partition), offset in offsets.items():
        kafka_offsets[TopicPartition(topic, partition)] = OffsetAndMetadata(
            offset,
            None
        )

    if kafka_offsets:
        consumer.commit(offsets=kafka_offsets)


# =========================================================
# Process batch: PostgreSQL first, Kafka second
# =========================================================

def process_batch(connection, consumer, batch):
    """
    Persist batch to PostgreSQL first, then commit Kafka offsets.

    If Kafka commit fails after PostgreSQL commit, the batch remains
    replayable and the DB unique constraint makes replay idempotent.
    """

    if not batch:
        return 0

    inserted = insert_batch(connection, batch)

    # PostgreSQL is now durable. Only now advance Kafka.
    commit_batch_offsets(consumer, batch)

    return inserted


# =========================================================
# Main DB writer
# =========================================================

def run():
    global running

    logger.info("==================================================")
    logger.info("Starting high-throughput Kafka → PostgreSQL DB Writer")
    logger.info("Kafka topic: %s", KAFKA_TOPIC)
    logger.info("Kafka bootstrap server: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Kafka consumer group: %s", KAFKA_GROUP_ID)
    logger.info(
        "PostgreSQL: %s:%s/%s",
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB
    )
    logger.info("PostgreSQL batch size: %d", BATCH_SIZE)
    logger.info("Kafka poll size: %d", KAFKA_POLL_RECORDS)
    logger.info(
        "Flush interval: %d seconds",
        FLUSH_INTERVAL_SECONDS
    )
    logger.info("PostgreSQL insertion: COPY + ON CONFLICT DO NOTHING")
    logger.info("Kafka offset mode: MANUAL")
    logger.info("==================================================")

    consumer = None
    connection = None
    batch = []

    last_flush = time.time()
    total_records = 0
    total_inserted = 0
    total_duplicates = 0
    total_batches = 0
    start_time = time.time()

    try:
        consumer = create_consumer()
        logger.info("Kafka consumer created successfully")

        connection = create_db_connection()
        logger.info("Connected to PostgreSQL successfully")

        prepare_staging_table(connection)

        logger.info("Waiting for messages from Kafka...")

        while running:
            records = consumer.poll(
                timeout_ms=1000,
                max_records=KAFKA_POLL_RECORDS
            )

            for _, messages in records.items():
                for message in messages:
                    try:
                        batch.append(message)
                    except Exception:
                        logger.exception(
                            "Failed to buffer Kafka message "
                            "partition=%s offset=%s",
                            message.partition,
                            message.offset
                        )
                        raise

            current_time = time.time()

            should_flush = (
                len(batch) >= BATCH_SIZE
                or (
                    len(batch) > 0
                    and current_time - last_flush >= FLUSH_INTERVAL_SECONDS
                )
            )

            if not should_flush:
                continue

            # Convert and persist only at flush time. This avoids
            # unnecessary per-message DB work while accumulating.
            rows = [message_to_row(message) for message in batch]

            # Replace messages with rows only for the DB operation.
            # Keep the original Kafka messages separately for offsets.
            messages_for_commit = list(batch)

            inserted = insert_batch(connection, rows)

            try:
                commit_batch_offsets(consumer, messages_for_commit)
            except Exception:
                logger.exception(
                    "Kafka offset commit failed AFTER PostgreSQL commit. "
                    "The batch will be replayed on restart; PostgreSQL "
                    "Kafka-offset uniqueness makes the replay safe."
                )
                raise

            batch.clear()
            last_flush = time.time()

            total_records += len(rows)
            total_inserted += inserted
            total_duplicates += len(rows) - inserted
            total_batches += 1

            elapsed = time.time() - start_time
            rate = total_inserted / elapsed if elapsed > 0 else 0.0

            logger.info(
                "Progress: received=%d | inserted=%d | duplicates=%d | "
                "batches=%d | pending=%d | avg_rate=%.2f records/sec",
                total_records,
                total_inserted,
                total_duplicates,
                total_batches,
                len(batch),
                rate
            )

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
        running = False

    except Exception:
        logger.exception("Fatal error in DB writer.")
        raise

    finally:
        # =====================================================
        # Final batch
        # =====================================================
        if batch:
            logger.info(
                "Flushing final batch of %d records...",
                len(batch)
            )

            try:
                rows = [message_to_row(message) for message in batch]
                messages_for_commit = list(batch)

                inserted = insert_batch(connection, rows)
                commit_batch_offsets(consumer, messages_for_commit)

                total_records += len(rows)
                total_inserted += inserted
                total_duplicates += len(rows) - inserted
                total_batches += 1

                batch.clear()

                logger.info(
                    "Final batch inserted and Kafka offsets "
                    "committed successfully"
                )

            except Exception:
                logger.exception(
                    "Failed to flush final batch. "
                    "Kafka offsets were NOT advanced."
                )

        # =====================================================
        # Close Kafka
        # =====================================================
        if consumer is not None:
            try:
                consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception:
                logger.exception("Error closing Kafka consumer.")

        # =====================================================
        # Close PostgreSQL
        # =====================================================
        if connection is not None:
            try:
                connection.close()
                logger.info("PostgreSQL connection closed.")
            except Exception:
                logger.exception("Error closing PostgreSQL connection.")

        # =====================================================
        # Final statistics
        # =====================================================
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0.0

        logger.info("==================================================")
        logger.info("DB writer stopped.")
        logger.info("Total records received: %d", total_records)
        logger.info("Total rows inserted: %d", total_inserted)
        logger.info("Duplicate rows skipped: %d", total_duplicates)
        logger.info("Total batches: %d", total_batches)
        logger.info("Elapsed time: %.2f seconds", elapsed)
        logger.info("Average insert rate: %.2f records/sec", rate)
        logger.info("==================================================")


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("DB writer terminated with an error.")
        sys.exit(1)
