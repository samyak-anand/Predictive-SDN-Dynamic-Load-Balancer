import json
import logging
import time
import uuid

from kafka import KafkaConsumer, KafkaProducer


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9092"

RAW_TOPIC = "traffic.raw"


# ============================================================
# TEST EVENT
# ============================================================


def create_test_event() -> dict:
    """
    Create a unique valid traffic event.
    """

    return {
        "event_id": f"retry_test_{uuid.uuid4()}",
        "timestamp": "2004-03-01T00:00:00",
        "source_node": "ATLAM5",
        "destination_node": "ATLAng",
        "traffic_mbps": 25.5,
        "demand_id": "ATLAM5_ATLAng",
        "granularity": "5min",
        "unit": "MBITPERSEC",
        "dataset": "abilene",
        "source_format": "sndlib_native",
        "source_folder": (
            "directed-abilene-zhang-5min-over-6months-ALL-native"
        ),
        "source_file": (
            "demandMatrix-abilene-zhang-5min-20040301-0000.txt"
        ),
        "schema_version": "1.0",
    }


# ============================================================
# PUBLISH MESSAGE
# ============================================================


def publish_test_message(event: dict):
    """
    Publish one unique test event to traffic.raw.
    """

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    try:

        future = producer.send(
            RAW_TOPIC,
            value=json.dumps(event).encode("utf-8"),
        )

        producer.flush()

        metadata = future.get(
            timeout=10
        )

        logger.info(
            "TEST MESSAGE WRITTEN | "
            "topic=%s | "
            "partition=%s | "
            "offset=%s | "
            "event_id=%s",
            metadata.topic,
            metadata.partition,
            metadata.offset,
            event["event_id"],
        )

        return metadata

    finally:

        producer.close()


# ============================================================
# CREATE CONSUMER
# ============================================================


def create_consumer(group_id: str):
    """
    Create a Kafka consumer with:

        - manual offset commits
        - raw byte consumption
        - earliest offset reset

    Raw bytes are used intentionally because traffic.raw
    can contain malformed JSON records.

    JSON deserialization is therefore handled manually
    inside consume_until_event().
    """

    return KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=None,
    )


# ============================================================
# SAFE JSON DECODING
# ============================================================


def decode_message(message):
    """
    Safely decode a Kafka message.

    Returns:
        dict
            When the message contains valid JSON object data.

        None
            When the message is malformed or is not a JSON object.

    The retry test must tolerate malformed historical records
    because traffic.raw is intentionally being used as a raw
    ingestion topic.
    """

    try:

        value = message.value

        if value is None:
            return None

        if isinstance(value, bytes):

            value = value.decode(
                "utf-8"
            )

        record = json.loads(
            value
        )

        if not isinstance(record, dict):

            logger.warning(
                "IGNORING NON-OBJECT JSON | "
                "partition=%s | "
                "offset=%s",
                message.partition,
                message.offset,
            )

            return None

        return record

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
    ) as exc:

        logger.warning(
            "IGNORING MALFORMED HISTORICAL MESSAGE | "
            "partition=%s | "
            "offset=%s | "
            "error=%s",
            message.partition,
            message.offset,
            exc,
        )

        return None


# ============================================================
# WAIT FOR TEST MESSAGE
# ============================================================


def consume_until_event(
    consumer,
    expected_event_id: str,
    timeout_seconds: int = 15,
):
    """
    Consume until the expected event is received.

    Historical malformed records are ignored because they
    are not relevant to this offset/retry test.

    The consumer does NOT commit offsets here.
    """

    start_time = time.time()

    while (
        time.time() - start_time
        < timeout_seconds
    ):

        records = consumer.poll(
            timeout_ms=1000
        )

        if not records:

            continue

        for _, messages in records.items():

            for message in messages:

                record = decode_message(
                    message
                )

                if record is None:

                    continue

                event_id = record.get(
                    "event_id"
                )

                logger.info(
                    "CONSUMED | "
                    "partition=%s | "
                    "offset=%s | "
                    "event_id=%s",
                    message.partition,
                    message.offset,
                    event_id,
                )

                if (
                    event_id
                    == expected_event_id
                ):

                    return message

    raise AssertionError(
        "Expected test event was not received "
        "within the timeout period."
    )


# ============================================================
# MAIN TEST
# ============================================================


def main():

    logger.info(
        "========================================"
    )

    logger.info(
        "STARTING KAFKA CONSUMER RETRY TEST"
    )

    logger.info(
        "========================================"
    )

    # ========================================================
    # STEP 1
    # Create unique consumer group
    # ========================================================

    group_id = (
        f"consumer-retry-test-{uuid.uuid4()}"
    )

    logger.info(
        "Consumer group: %s",
        group_id,
    )

    # ========================================================
    # STEP 2
    # Create unique event
    # ========================================================

    event = create_test_event()

    expected_event_id = event[
        "event_id"
    ]

    logger.info(
        "Test event ID: %s",
        expected_event_id,
    )

    # ========================================================
    # STEP 3
    # Publish event
    # ========================================================

    logger.info(
        "STEP 1: Publishing test event..."
    )

    metadata = publish_test_message(
        event
    )

    test_partition = metadata.partition
    test_offset = metadata.offset

    logger.info(
        "PASS: Test event published."
    )

    # ========================================================
    # STEP 4
    # First consumer
    #
    # Simulate application failure:
    #
    # - consume message
    # - DO NOT commit
    # - close consumer
    # ========================================================

    logger.info(
        "STEP 2: Simulating processing failure..."
    )

    consumer = create_consumer(
        group_id
    )

    try:

        message = consume_until_event(
            consumer=consumer,
            expected_event_id=expected_event_id,
        )

        assert (
            message.partition
            == test_partition
        ), (
            "Partition changed unexpectedly."
        )

        assert (
            message.offset
            == test_offset
        ), (
            "Offset changed unexpectedly."
        )

        logger.info(
            "MESSAGE RECEIVED BEFORE FAILURE | "
            "partition=%s | "
            "offset=%s | "
            "event_id=%s",
            message.partition,
            message.offset,
            expected_event_id,
        )

        logger.info(
            "SIMULATED FAILURE: "
            "application crashes before commit."
        )

        # IMPORTANT:
        #
        # DO NOT commit here.
        #
        # This is intentionally simulating:
        #
        #     message received
        #          ↓
        #     processing fails
        #          ↓
        #     application crashes
        #          ↓
        #     NO COMMIT

    finally:

        consumer.close()

    logger.info(
        "PASS: Consumer stopped without committing."
    )

    # ========================================================
    # STEP 5
    # Allow Kafka group state to settle
    # ========================================================

    time.sleep(1)

    # ========================================================
    # STEP 6
    # Restart consumer with SAME group ID
    # ========================================================

    logger.info(
        "STEP 3: Restarting consumer with same group..."
    )

    retry_consumer = create_consumer(
        group_id
    )

    try:

        retry_message = consume_until_event(
            consumer=retry_consumer,
            expected_event_id=expected_event_id,
            timeout_seconds=15,
        )

        logger.info(
            "RETRY MESSAGE RECEIVED | "
            "partition=%s | "
            "offset=%s | "
            "event_id=%s",
            retry_message.partition,
            retry_message.offset,
            expected_event_id,
        )

        # ====================================================
        # STEP 7
        # Verify same message was replayed
        # ====================================================

        assert (
            retry_message.partition
            == test_partition
        ), (
            "Retry occurred on a different partition."
        )

        assert (
            retry_message.offset
            == test_offset
        ), (
            "Kafka did not replay the uncommitted offset."
        )

        record = decode_message(
            retry_message
        )

        assert record is not None, (
            "Retry message could not be decoded."
        )

        assert (
            record["event_id"]
            == expected_event_id
        ), (
            "Retry event ID does not match."
        )

        logger.info(
            "PASS: Uncommitted message was replayed."
        )

        # ====================================================
        # STEP 8
        # Simulate successful processing
        # ====================================================

        logger.info(
            "STEP 4: Simulating successful processing..."
        )

        retry_consumer.commit()

        logger.info(
            "OFFSET COMMITTED | "
            "partition=%s | "
            "committed_offset=%s",
            retry_message.partition,
            retry_message.offset + 1,
        )

        logger.info(
            "PASS: Offset committed after successful processing."
        )

    finally:

        retry_consumer.close()

    # ========================================================
    # STEP 9
    # Start third consumer with SAME group
    #
    # The already-committed message should NOT be replayed.
    # ========================================================

    logger.info(
        "STEP 5: Verifying committed offset..."
    )

    verification_consumer = create_consumer(
        group_id
    )

    try:

        verification_start = time.time()

        replayed_after_commit = False

        while (
            time.time() - verification_start
            < 5
        ):

            records = verification_consumer.poll(
                timeout_ms=1000
            )

            if not records:

                continue

            for _, messages in records.items():

                for message in messages:

                    record = decode_message(
                        message
                    )

                    if record is None:

                        continue

                    event_id = record.get(
                        "event_id"
                    )

                    logger.info(
                        "POST-COMMIT MESSAGE | "
                        "partition=%s | "
                        "offset=%s | "
                        "event_id=%s",
                        message.partition,
                        message.offset,
                        event_id,
                    )

                    if (
                        event_id
                        == expected_event_id
                    ):

                        replayed_after_commit = True

        assert not replayed_after_commit, (
            "Message was replayed after the "
            "offset had been committed."
        )

        logger.info(
            "PASS: Committed message was not replayed."
        )

    finally:

        verification_consumer.close()

    # ========================================================
    # FINAL RESULT
    # ========================================================

    logger.info(
        "========================================"
    )

    logger.info(
        "ALL KAFKA CONSUMER RETRY TESTS PASSED"
    )

    logger.info(
        "========================================"
    )

    print(
        "Kafka consumer retry test "
        "completed successfully."
    )


# ============================================================
# ENTRY POINT
# ============================================================


if __name__ == "__main__":

    main()