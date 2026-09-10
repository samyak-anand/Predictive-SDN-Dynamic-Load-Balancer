#```python
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

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "traffic.validated"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "postgres-db-writer"
)

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "sdn_traffic"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "sdn_user"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "sdn_password"
)

# Large batch for 6M+ records
BATCH_SIZE = int(
    os.getenv(
        "DB_BATCH_SIZE",
        "50000"
    )
)

FLUSH_INTERVAL_SECONDS = int(
    os.getenv(
        "DB_FLUSH_INTERVAL_SECONDS",
        "5"
    )
)


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
        "Shutdown signal received. "
        "Finishing current batch..."
    )

    running = False


signal.signal(
    signal.SIGINT,
    shutdown_handler
)

signal.signal(
    signal.SIGTERM,
    shutdown_handler
)


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

    connection = psycopg.connect(
        connection_string
    )

    # Explicit transaction control
    connection.autocommit = False

    return connection


# =========================================================
# Generic value extraction
# =========================================================

def get_value(payload, *keys):
    """
    Return the first available value from the payload.
    """

    for key in keys:

        if key in payload:

            return payload[key]

    return None


# =========================================================
# Timestamp conversion
# =========================================================

def convert_timestamp(value):
    """
    PostgreSQL TIMESTAMPTZ can accept ISO-8601 timestamp
    strings directly through psycopg.
    """

    if value is None:

        return None

    return value


# =========================================================
# Kafka message -> PostgreSQL row
# =========================================================

def message_to_row(message):
    """
    Convert one Kafka JSON message into a PostgreSQL row.
    """

    payload = message.value

    if not isinstance(payload, dict):

        raise ValueError(
            f"Expected JSON object but received "
            f"{type(payload).__name__}"
        )

    # -----------------------------------------------------
    # Event ID
    # -----------------------------------------------------

    event_id = get_value(
        payload,
        "event_id",
        "eventId",
        "id"
    )

    if event_id is None:

        raise ValueError(
            "Validated message does not contain event_id"
        )

    # -----------------------------------------------------
    # Timestamp
    # -----------------------------------------------------

    event_timestamp = convert_timestamp(
        get_value(
            payload,
            "event_timestamp",
            "timestamp",
            "event_time",
            "eventTime"
        )
    )

    # -----------------------------------------------------
    # Nodes
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # Network information
    # -----------------------------------------------------

    source_ip = get_value(
        payload,
        "source_ip",
        "sourceIp",
        "src_ip"
    )

    destination_ip = get_value(
        payload,
        "destination_ip",
        "destinationIp",
        "dst_ip"
    )

    protocol = get_value(
        payload,
        "protocol"
    )

    # -----------------------------------------------------
    # Traffic counters
    # -----------------------------------------------------

    packet_count = get_value(
        payload,
        "packet_count",
        "packetCount"
    )

    byte_count = get_value(
        payload,
        "byte_count",
        "byteCount"
    )

    duration_ms = get_value(
        payload,
        "duration_ms",
        "durationMs"
    )

    # -----------------------------------------------------
    # Traffic metrics
    # -----------------------------------------------------

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

    latency_ms = get_value(
        payload,
        "latency_ms",
        "latencyMs"
    )

    # -----------------------------------------------------
    # Raw JSON
    # -----------------------------------------------------

    raw_payload = json.dumps(
        payload,
        ensure_ascii=False
    )

    # -----------------------------------------------------
    # PostgreSQL row
    # -----------------------------------------------------

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
# PostgreSQL INSERT query
# =========================================================

INSERT_QUERY = """
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
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
ON CONFLICT (
    kafka_topic,
    kafka_partition,
    kafka_offset
)
DO NOTHING
"""


# =========================================================
# Batch insertion
# =========================================================

def insert_batch(connection, batch):
    """
    Insert a complete batch inside one PostgreSQL
    transaction.

    Returns only after PostgreSQL successfully commits.

    Kafka offsets must be committed by the caller AFTER
    this function succeeds.
    """

    if not batch:

        return

    batch_size = len(batch)

    logger.info(
        "Inserting batch of %d records into PostgreSQL...",
        batch_size
    )

    try:

        with connection.cursor() as cursor:

            cursor.executemany(
                INSERT_QUERY,
                batch
            )

        # -------------------------------------------------
        # Commit PostgreSQL transaction
        # -------------------------------------------------

        connection.commit()

        logger.info(
            "Inserted batch of %d records into PostgreSQL",
            batch_size
        )

    except Exception:

        # -------------------------------------------------
        # Roll back PostgreSQL transaction
        # -------------------------------------------------

        connection.rollback()

        logger.exception(
            "Database insertion failed. "
            "Transaction rolled back."
        )

        # -------------------------------------------------
        # CRITICAL:
        # Do not allow Kafka offset commit.
        # -------------------------------------------------

        raise


# =========================================================
# Kafka consumer
# =========================================================

def create_consumer():

    consumer = KafkaConsumer(

        KAFKA_TOPIC,

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        group_id=KAFKA_GROUP_ID,

        # -------------------------------------------------
        # Manual offset management
        # -------------------------------------------------

        enable_auto_commit=False,

        auto_offset_reset="earliest",

        # -------------------------------------------------
        # JSON decoder
        # -------------------------------------------------

        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),

        # -------------------------------------------------
        # Large ingestion settings
        # -------------------------------------------------

        max_poll_records=BATCH_SIZE,

        # Allow enough time for PostgreSQL batch insert
        max_poll_interval_ms=600000,

        # Network/session settings
        session_timeout_ms=45000,

        request_timeout_ms=60000
    )

    return consumer


# =========================================================
# Main DB writer
# =========================================================

def run():

    logger.info(
        "=================================================="
    )

    logger.info(
        "Starting Kafka → PostgreSQL DB Writer"
    )

    logger.info(
        "Kafka topic: %s",
        KAFKA_TOPIC
    )

    logger.info(
        "Kafka bootstrap server: %s",
        KAFKA_BOOTSTRAP_SERVERS
    )

    logger.info(
        "Kafka consumer group: %s",
        KAFKA_GROUP_ID
    )

    logger.info(
        "PostgreSQL: %s:%s/%s",
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB
    )

    logger.info(
        "Batch size: %d",
        BATCH_SIZE
    )

    logger.info(
        "Flush interval: %d seconds",
        FLUSH_INTERVAL_SECONDS
    )

    logger.info(
        "=================================================="
    )

    consumer = None
    connection = None

    batch = []

    last_flush = time.time()

    total_records = 0
    total_batches = 0

    start_time = time.time()

    try:

        # -------------------------------------------------
        # Create Kafka consumer
        # -------------------------------------------------

        consumer = create_consumer()

        logger.info(
            "Kafka consumer created successfully"
        )

        # -------------------------------------------------
        # PostgreSQL connection
        # -------------------------------------------------

        connection = create_db_connection()

        logger.info(
            "Connected to PostgreSQL successfully"
        )

        logger.info(
            "Waiting for messages from Kafka..."
        )

        # -------------------------------------------------
        # Main loop
        # -------------------------------------------------

        while running:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=BATCH_SIZE
            )

            # ------------------------------------------------
            # Process Kafka records
            # ------------------------------------------------

            for _, messages in records.items():

                for message in messages:

                    try:

                        row = message_to_row(
                            message
                        )

                        batch.append(row)

                    except Exception:

                        logger.exception(
                            "Failed to transform Kafka message "
                            "partition=%s offset=%s",
                            message.partition,
                            message.offset
                        )

                        raise

            # ------------------------------------------------
            # Check whether batch should be flushed
            # ------------------------------------------------

            current_time = time.time()

            should_flush = (
                len(batch) >= BATCH_SIZE
                or (
                    len(batch) > 0
                    and (
                        current_time - last_flush
                        >= FLUSH_INTERVAL_SECONDS
                    )
                )
            )

            if not should_flush:

                continue

            # ------------------------------------------------
            # Insert into PostgreSQL
            # ------------------------------------------------

            insert_batch(
                connection,
                batch
            )

            # ------------------------------------------------
            # IMPORTANT:
            #
            # PostgreSQL transaction has successfully
            # committed.
            #
            # NOW commit Kafka offsets.
            # ------------------------------------------------

            consumer.commit()

            total_records += len(batch)

            total_batches += 1

            elapsed = time.time() - start_time

            rate = (
                total_records / elapsed
                if elapsed > 0
                else 0
            )

            logger.info(
                "Kafka offsets committed successfully"
            )

            logger.info(
                "Progress: %d records | "
                "batches: %d | "
                "rate: %.2f records/sec",
                total_records,
                total_batches,
                rate
            )

            # ------------------------------------------------
            # Clear successfully committed batch
            # ------------------------------------------------

            batch.clear()

            last_flush = time.time()

    except KeyboardInterrupt:

        logger.info(
            "Keyboard interrupt received."
        )

    except Exception:

        logger.exception(
            "Fatal error in DB writer."
        )

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

                insert_batch(
                    connection,
                    batch
                )

                # DB commit succeeded.
                # Now commit Kafka offsets.

                consumer.commit()

                total_records += len(batch)

                logger.info(
                    "Final batch inserted and Kafka "
                    "offsets committed successfully"
                )

            except Exception:

                logger.exception(
                    "Failed to flush final batch. "
                    "Kafka offsets were NOT committed."
                )

        # =====================================================
        # Close Kafka
        # =====================================================

        if consumer is not None:

            try:

                consumer.close()

                logger.info(
                    "Kafka consumer closed."
                )

            except Exception:

                logger.exception(
                    "Error closing Kafka consumer."
                )

        # =====================================================
        # Close PostgreSQL
        # =====================================================

        if connection is not None:

            try:

                connection.close()

                logger.info(
                    "PostgreSQL connection closed."
                )

            except Exception:

                logger.exception(
                    "Error closing PostgreSQL connection."
                )

        # =====================================================
        # Final statistics
        # =====================================================

        elapsed = time.time() - start_time

        rate = (
            total_records / elapsed
            if elapsed > 0
            else 0
        )

        logger.info(
            "=================================================="
        )

        logger.info(
            "DB writer stopped."
        )

        logger.info(
            "Total records processed: %d",
            total_records
        )

        logger.info(
            "Total batches: %d",
            total_batches
        )

        logger.info(
            "Elapsed time: %.2f seconds",
            elapsed
        )

        logger.info(
            "Average rate: %.2f records/sec",
            rate
        )

        logger.info(
            "=================================================="
        )


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":

    try:

        run()

    except Exception:

        logger.exception(
            "DB writer terminated with an error."
        )

        sys.exit(1)
#```
