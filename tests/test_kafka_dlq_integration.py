import json
import logging
import time
import uuid
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer

from src.kafka.consumer import TrafficKafkaConsumer


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

DLQ_TOPIC = "traffic.dlq"


# ============================================================
# TEST EVENT FACTORIES
# ============================================================


def create_valid_event() -> dict:
    """
    Create a valid TrafficEvent payload.
    """

    return {
        "event_id": (
            f"dlq_integration_valid_{uuid.uuid4()}"
        ),
        "timestamp": datetime(
            2004,
            3,
            1,
            0,
            0,
        ).isoformat(),
        "source_node": "ATLAM5",
        "destination_node": "ATLAng",
        "traffic_mbps": 10.5,
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


def create_missing_field_event() -> dict:
    """
    Create an invalid event with the required demand_id
    field removed.
    """

    event = create_valid_event()

    del event["demand_id"]

    event["event_id"] = (
        f"dlq_integration_missing_{uuid.uuid4()}"
    )

    return event


def create_negative_traffic_event() -> dict:
    """
    Create an invalid event with negative traffic.
    """

    event = create_valid_event()

    event["event_id"] = (
        f"dlq_integration_negative_{uuid.uuid4()}"
    )

    event["traffic_mbps"] = -10.0

    return event


# ============================================================
# PUBLISH TEST MESSAGES
# ============================================================


def publish_test_messages() -> dict:
    """
    Publish four integration-test messages to traffic.raw.

    Messages:

        1. Valid event
        2. Invalid JSON
        3. Missing required field
        4. Negative traffic

    Returns the event IDs needed for verification.
    """

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    valid_event = create_valid_event()

    missing_field_event = (
        create_missing_field_event()
    )

    negative_traffic_event = (
        create_negative_traffic_event()
    )

    messages = [
        (
            "valid_event",
            json.dumps(
                valid_event
            ).encode("utf-8"),
        ),
        (
            "invalid_json",
            b"THIS IS NOT VALID JSON",
        ),
        (
            "missing_field",
            json.dumps(
                missing_field_event
            ).encode("utf-8"),
        ),
        (
            "negative_traffic",
            json.dumps(
                negative_traffic_event
            ).encode("utf-8"),
        ),
    ]

    futures = []

    try:

        for name, payload in messages:

            future = producer.send(
                RAW_TOPIC,
                value=payload,
            )

            futures.append(
                (
                    name,
                    future,
                )
            )

        producer.flush()

        logger.info(
            "Published %d integration-test messages",
            len(messages),
        )

        # Verify every message was actually written.
        for name, future in futures:

            metadata = future.get(
                timeout=10
            )

            logger.info(
                "TEST MESSAGE WRITTEN | "
                "name=%s | "
                "topic=%s | "
                "partition=%s | "
                "offset=%s",
                name,
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )

    finally:

        producer.close()

    return {
        "valid_event_id": (
            valid_event["event_id"]
        ),
        "missing_field_event_id": (
            missing_field_event["event_id"]
        ),
        "negative_event_id": (
            negative_traffic_event["event_id"]
        ),
    }


# ============================================================
# CONSUME RAW TOPIC
# ============================================================


def consume_test_messages(
    expected_valid_event_id: str,
):
    """
    Consume traffic.raw using a unique consumer group.

    IMPORTANT:

    We intentionally do NOT stop when the valid event is
    received.

    The four test messages can be distributed across
    different Kafka partitions. If we stop immediately
    after finding the valid event, the invalid messages
    may never be processed and therefore never reach
    the DLQ.

    TrafficKafkaConsumer internally handles:
        - JSON deserialization
        - Event reconstruction
        - Validation
        - DLQ routing

    This function only collects valid events that reach
    downstream processing.
    """

    group_id = (
        f"dlq-integration-test-{uuid.uuid4()}"
    )

    consumer = TrafficKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=RAW_TOPIC,
        group_id=group_id,
        auto_offset_reset="earliest",
    )

    consumed_valid_events = []

    valid_event_found = False

    logger.info(
        "Starting raw-topic integration consumer | "
        "group_id=%s",
        group_id,
    )

    # TrafficKafkaConsumer.consume() already closes
    # the consumer in its own finally block.
    for event in consumer.consume():

        consumed_valid_events.append(
            event
        )

        logger.info(
            "DOWNSTREAM EVENT | "
            "event_id=%s",
            event.event_id,
        )

        if (
            event.event_id
            == expected_valid_event_id
        ):

            valid_event_found = True

            logger.info(
                "Expected valid event received."
            )

        # IMPORTANT:
        #
        # DO NOT BREAK HERE.
        #
        # We must allow TrafficKafkaConsumer to continue
        # processing the other partitions so that the
        # invalid test messages reach traffic.dlq.

    logger.info(
        "Raw-topic integration consumption completed | "
        "valid_event_found=%s | "
        "valid_events=%d",
        valid_event_found,
        len(consumed_valid_events),
    )

    return consumed_valid_events


# ============================================================
# CONSUME DLQ
# ============================================================


def consume_dlq_messages(
    expected_event_ids: set[str],
    timeout_seconds: int = 15,
):
    """
    Consume traffic.dlq and find the invalid events created
    by the current integration-test execution.

    The DLQ already contains historical test records.

    Therefore, this function ignores every DLQ record whose
    event_id does not belong to the current test.

    The function exits early once all expected event IDs
    have been found.
    """

    group_id = (
        f"dlq-verification-{uuid.uuid4()}"
    )

    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    received_records = []

    received_ids = set()

    start_time = time.time()

    logger.info(
        "Starting DLQ verification | "
        "expected_event_ids=%s",
        expected_event_ids,
    )

    try:

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

                    record = message.value

                    event_id = record.get(
                        "event_id"
                    )

                    error_type = record.get(
                        "error_type"
                    )

                    validation_stage = record.get(
                        "validation_stage"
                    )

                    logger.info(
                        "DLQ RECORD | "
                        "event_id=%s | "
                        "error_type=%s | "
                        "validation_stage=%s",
                        event_id,
                        error_type,
                        validation_stage,
                    )

                    # Ignore historical DLQ records.
                    if (
                        event_id
                        not in expected_event_ids
                    ):

                        continue

                    # Prevent duplicate records from
                    # being counted twice.
                    if (
                        event_id
                        in received_ids
                    ):

                        continue

                    received_records.append(
                        record
                    )

                    received_ids.add(
                        event_id
                    )

                    logger.info(
                        "EXPECTED DLQ EVENT FOUND | "
                        "event_id=%s | "
                        "received=%d/%d",
                        event_id,
                        len(received_ids),
                        len(expected_event_ids),
                    )

            # We found every expected invalid event.
            if (
                received_ids
                == expected_event_ids
            ):

                logger.info(
                    "All expected DLQ events found."
                )

                break

    finally:

        consumer.close()

    logger.info(
        "DLQ verification completed | "
        "expected=%s | "
        "received=%s",
        expected_event_ids,
        received_ids,
    )

    return received_records


# ============================================================
# MAIN TEST
# ============================================================


def main():

    logger.info(
        "========================================"
    )

    logger.info(
        "STARTING KAFKA DLQ INTEGRATION TEST"
    )

    logger.info(
        "========================================"
    )

    # ========================================================
    # STEP 1
    # Publish test messages
    # ========================================================

    logger.info(
        "STEP 1: Publishing test messages..."
    )

    test_ids = publish_test_messages()

    logger.info(
        "Test messages published successfully."
    )

    logger.info(
        "VALID EVENT ID: %s",
        test_ids["valid_event_id"],
    )

    logger.info(
        "MISSING FIELD EVENT ID: %s",
        test_ids["missing_field_event_id"],
    )

    logger.info(
        "NEGATIVE TRAFFIC EVENT ID: %s",
        test_ids["negative_event_id"],
    )

    # ========================================================
    # STEP 2
    # Consume traffic.raw
    # ========================================================

    logger.info(
        "STEP 2: Consuming traffic.raw..."
    )

    valid_events = consume_test_messages(
        expected_valid_event_id=(
            test_ids["valid_event_id"]
        )
    )

    valid_event_ids = {
        event.event_id
        for event in valid_events
    }

    # Verify the valid event reached downstream.
    assert (
        test_ids["valid_event_id"]
        in valid_event_ids
    ), (
        "Valid event did not reach downstream."
    )

    logger.info(
        "PASS: Valid event reached downstream."
    )

    # ========================================================
    # STEP 3
    # Verify DLQ
    # ========================================================

    logger.info(
        "STEP 3: Verifying traffic.dlq..."
    )

    expected_dlq_ids = {
        test_ids[
            "missing_field_event_id"
        ],
        test_ids[
            "negative_event_id"
        ],
    }

    logger.info(
        "EXPECTED DLQ IDS: %s",
        expected_dlq_ids,
    )

    dlq_records = consume_dlq_messages(
        expected_event_ids=expected_dlq_ids,
        timeout_seconds=15,
    )

    dlq_event_ids = {
        record.get("event_id")
        for record in dlq_records
    }

    logger.info(
        "RECEIVED DLQ IDS: %s",
        dlq_event_ids,
    )

    # Verify both expected invalid events.
    assert (
        expected_dlq_ids
        <= dlq_event_ids
    ), (
        "Expected invalid events were not "
        "found in the DLQ."
    )

    logger.info(
        "PASS: Invalid events reached DLQ."
    )

    # ========================================================
    # STEP 4
    # Validate DLQ metadata
    # ========================================================

    logger.info(
        "STEP 4: Validating DLQ metadata..."
    )

    required_dlq_fields = {
        "original_topic",
        "partition",
        "offset",
        "error_type",
        "error_message",
        "validation_stage",
        "payload_base64",
    }

    for record in dlq_records:

        # Original Kafka topic.
        assert (
            record["original_topic"]
            == RAW_TOPIC
        )

        # Check required metadata fields.
        for field in required_dlq_fields:

            assert field in record, (
                f"DLQ record missing required "
                f"field: {field}"
            )

    logger.info(
        "PASS: DLQ metadata and original "
        "payload were preserved."
    )

    # ========================================================
    # STEP 5
    # Validate specific error types
    # ========================================================

    logger.info(
        "STEP 5: Validating DLQ error classification..."
    )

    records_by_event_id = {
        record["event_id"]: record
        for record in dlq_records
    }

    missing_record = records_by_event_id[
        test_ids["missing_field_event_id"]
    ]

    negative_record = records_by_event_id[
        test_ids["negative_event_id"]
    ]

    # Missing demand_id should fail during
    # event reconstruction.
    assert (
        missing_record["error_type"]
        == "KeyError"
    )

    assert (
        missing_record["validation_stage"]
        == "event_reconstruction"
    )

    # Negative traffic should fail during
    # traffic validation.
    assert (
        negative_record["error_type"]
        == "TrafficValidationError"
    )

    assert (
        negative_record["validation_stage"]
        == "traffic_validation"
    )

    logger.info(
        "PASS: DLQ error classification is correct."
    )

    # ========================================================
    # FINAL RESULT
    # ========================================================

    logger.info(
        "========================================"
    )

    logger.info(
        "ALL KAFKA DLQ INTEGRATION TESTS PASSED"
    )

    logger.info(
        "========================================"
    )

    print(
        "Kafka DLQ integration test "
        "completed successfully."
    )


# ============================================================
# ENTRY POINT
# ============================================================


if __name__ == "__main__":

    main()