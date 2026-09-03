import base64
import json
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer


logger = logging.getLogger(__name__)


class TrafficDLQProducer:
    """
    Kafka producer responsible for publishing failed
    traffic messages to the Dead Letter Queue.

    DLQ topic:
        traffic.dlq

    The DLQ preserves:
        - Original Kafka topic
        - Original partition
        - Original offset
        - Original payload
        - Event ID when available
        - Error type
        - Error message
        - Validation stage
        - DLQ timestamp

    This allows failed records to be:
        - Investigated
        - Reprocessed
        - Replayed
        - Audited

    Does NOT:
        - Validate events
        - Parse XML/TXT
        - Perform ML
        - Perform SDN decisions
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "traffic.dlq",
    ):
        """
        Initialize the DLQ Kafka producer.

        Args:
            bootstrap_servers:
                Kafka broker address.

            topic:
                Kafka DLQ topic.
        """

        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,

            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),

            acks="all",

            retries=5,

            linger_ms=10,

            batch_size=32768,

            compression_type="gzip",
        )

        logger.info(
            "DLQ producer initialized | "
            "bootstrap_servers=%s | "
            "topic=%s",
            bootstrap_servers,
            topic,
        )

    @staticmethod
    def build_dlq_record(
        *,
        original_topic: str,
        partition: int,
        offset: int,
        payload: bytes,
        error_type: str,
        error_message: str,
        validation_stage: str,
        event_id: str | None = None,
    ) -> dict:
        """
        Build the DLQ record.

        The original Kafka payload is preserved in two forms:

        1. payload_text
           Useful when the payload is valid UTF-8.

        2. payload_base64
           Preserves the exact original bytes, including
           malformed or non-UTF-8 messages.
        """

        try:
            payload_text = payload.decode(
                "utf-8"
            )

        except UnicodeDecodeError:
            payload_text = None

        return {
            "dlq_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),

            "original_topic": original_topic,

            "partition": partition,

            "offset": offset,

            "event_id": event_id,

            "error_type": error_type,

            "error_message": error_message,

            "validation_stage": validation_stage,

            "payload_text": payload_text,

            "payload_base64": base64.b64encode(
                payload
            ).decode("ascii"),
        }

    def send(
        self,
        *,
        original_topic: str,
        partition: int,
        offset: int,
        payload: bytes,
        error_type: str,
        error_message: str,
        validation_stage: str,
        event_id: str | None = None,
    ):
        """
        Publish a failed Kafka message to the DLQ.
        """

        dlq_record = self.build_dlq_record(
            original_topic=original_topic,
            partition=partition,
            offset=offset,
            payload=payload,
            error_type=error_type,
            error_message=error_message,
            validation_stage=validation_stage,
            event_id=event_id,
        )

        future = self.producer.send(
            self.topic,
            value=dlq_record,
        )

        future.add_callback(
            self._delivery_callback(
                original_topic=original_topic,
                partition=partition,
                offset=offset,
            )
        )

        future.add_errback(
            self._delivery_error_callback(
                original_topic=original_topic,
                partition=partition,
                offset=offset,
            )
        )

        return future

    @staticmethod
    def _delivery_callback(
        *,
        original_topic: str,
        partition: int,
        offset: int,
    ):
        """
        Callback executed after successful DLQ delivery.
        """

        def callback(metadata):

            logger.info(
                "DLQ ENTRY WRITTEN | "
                "original_topic=%s | "
                "original_partition=%s | "
                "original_offset=%s | "
                "dlq_topic=%s | "
                "dlq_partition=%s | "
                "dlq_offset=%s",
                original_topic,
                partition,
                offset,
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )

        return callback

    @staticmethod
    def _delivery_error_callback(
        *,
        original_topic: str,
        partition: int,
        offset: int,
    ):
        """
        Callback executed when DLQ delivery fails.
        """

        def callback(exception):

            logger.error(
                "DLQ WRITE FAILED | "
                "original_topic=%s | "
                "original_partition=%s | "
                "original_offset=%s | "
                "error=%s",
                original_topic,
                partition,
                offset,
                exception,
            )

        return callback

    def flush(self):
        """
        Wait for all pending DLQ messages
        to be acknowledged by Kafka.
        """

        logger.info(
            "Flushing DLQ producer..."
        )

        self.producer.flush()

    def close(self):
        """
        Gracefully close the DLQ producer.
        """

        logger.info(
            "Closing DLQ producer..."
        )

        self.producer.flush()
        self.producer.close()

        logger.info(
            "DLQ producer closed."
        )
        