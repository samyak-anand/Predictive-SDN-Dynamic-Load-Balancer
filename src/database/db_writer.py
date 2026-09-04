import json
import logging
import os
import signal
import time

import psycopg
from psycopg import sql
from kafka import KafkaConsumer


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

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

BATCH_SIZE = int(
    os.getenv("DB_BATCH_SIZE", "5000")
)

FLUSH_INTERVAL_SECONDS = int(
    os.getenv("DB_FLUSH_INTERVAL_SECONDS", "5")
)


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("db_writer")


# ---------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------

running = True


def shutdown_handler(signum, frame):
    global running

    logger.info(
        "Shutdown signal received. Finishing current batch..."
    )

    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ---------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------

def create_db_connection():
    connection_string = (
        f"host={POSTGRES_HOST} "
        f"port={POSTGRES_PORT} "
        f"dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} "
        f"password={POSTGRES_PASSWORD}"
    )

    return psycopg.connect(connection_string)


# ---------------------------------------------------------
# Value conversion
# ---------------------------------------------------------

def get_value(payload, *keys):
    """
    Return the first available value from the payload.

    Allows the writer to handle slightly different
    field naming conventions.
    """

    for key in keys:
        if key in payload:
            return payload[key]

    return None


def convert_timestamp(value):
    """
    PostgreSQL TIMESTAMPTZ accepts ISO timestamp strings.
    Return None when the value is missing.
    """

    if value is None:
        return None

    return value


# ---------------------------------------------------------
# Kafka message -> PostgreSQL row
# ---------------------------------------------------------

def message_to_row(message):
    """
    Convert one validated Kafka message into a PostgreSQL row.
    """

    payload = message.value

    if not isinstance(payload, dict):
        raise ValueError(
            f"Expected JSON object but received "
            f"{type(payload).__name__}"
        )

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

    row = (
        str(event_id),

        convert_timestamp(
            get_value(
                payload,
                "event_timestamp",
                "timestamp",
                "event_time",
                "eventTime"
            )
        ),

        get_value(
            payload,
            "source_node",
            "sourceNode",
            "src_node"
        ),

        get_value(
            payload,
            "destination_node",
            "destinationNode",
            "dst_node"
        ),

        get_value(
            payload,
            "source_ip",
            "sourceIp",
            "src_ip"
        ),

        get_value(
            payload,
            "destination_ip",
            "destinationIp",
            "dst_ip"
        ),

        get_value(
            payload,
            "protocol"
        ),

        get_value(
            payload,
            "packet_count",
            "packetCount"
        ),

        get_value(
            payload,
            "byte_count",
            "byteCount"
        ),

        get_value(
            payload,
            "duration_ms",
            "durationMs"
        ),

        get_value(
            payload,
            "throughput_mbps",
            "throughputMbps"
        ),

        get_value(
            payload,
            "packet_loss_percent",
            "packetLossPercent"
        ),

        get_value(
            payload,
            "latency_ms",
            "latencyMs"
        ),

        json.dumps(payload),

        message.topic,
        message.partition,
        message.offset
    )

    return row


# ---------------------------------------------------------
# Batch insert
# ---------------------------------------------------------

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
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s
)
ON CONFLICT (
    kafka_topic,
    kafka_partition,
    kafka_offset
)
DO NOTHING
"""


def insert_batch(connection, batch):
    """
    Insert a complete batch inside one PostgreSQL transaction.

    The Kafka offsets are committed only AFTER this function
    successfully commits the database transaction.
    """

    if not batch:
        return

    try:

        with connection.cursor() as cursor:

            cursor.executemany(
                INSERT_QUERY,
                batch
            )

        connection.commit()

        logger.info(
            "Inserted batch of %d records into PostgreSQL",
            len(batch)
        )

    except Exception:

        connection.rollback()

        logger.exception(
            "Database insertion failed. "
            "Transaction rolled back."
        )

        raise


# ---------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------

def create_consumer():

    return KafkaConsumer(
        KAFKA_TOPIC,

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        group_id=KAFKA_GROUP_ID,

        enable_auto_commit=False,

        auto_offset_reset="earliest",

        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),

        consumer_timeout_ms=1000,

        max_poll_records=BATCH_SIZE
    )


# ---------------------------------------------------------
# Main DB writer
# ---------------------------------------------------------

def run():

    logger.info("Starting Kafka → PostgreSQL DB Writer")

    logger.info(
        "Kafka topic: %s",
        KAFKA_TOPIC
    )

    logger.info(
        "Kafka bootstrap server: %s",
        KAFKA_BOOTSTRAP_SERVERS
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

    consumer = create_consumer()

    connection = create_db_connection()

    logger.info(
        "Connected to PostgreSQL successfully"
    )

    batch = []

    last_flush = time.time()

    try:

        while running:

            records = consumer.poll(
                timeout_ms=1000,
                max_records=BATCH_SIZE
            )

            for _, messages in records.items():

                for message in messages:

                    try:

                        row = message_to_row(message)

                        batch.append(row)

                    except Exception:

                        logger.exception(
                            "Failed to transform Kafka message "
                            "partition=%s offset=%s",
                            message.partition,
                            message.offset
                        )

            current_time = time.time()

            should_flush = (
                len(batch) >= BATCH_SIZE
                or (
                    batch
                    and
                    current_time - last_flush
                    >= FLUSH_INTERVAL_SECONDS
                )
            )

            if should_flush:

                insert_batch(
                    connection,
                    batch
                )

                # IMPORTANT:
                # Kafka offsets are committed ONLY AFTER
                # PostgreSQL transaction succeeds.

                consumer.commit()

                logger.info(
                    "Kafka offsets committed successfully"
                )

                batch.clear()

                last_flush = current_time

    except KeyboardInterrupt:

        logger.info(
            "Keyboard interrupt received."
        )

    finally:

        # Flush remaining messages during shutdown
        if batch:

            try:

                insert_batch(
                    connection,
                    batch
                )

                consumer.commit()

                logger.info(
                    "Final batch inserted and offsets committed"
                )

            except Exception:

                logger.exception(
                    "Failed to flush final batch"
                )

        consumer.close()

        connection.close()

        logger.info(
            "DB writer stopped."
        )


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    run()