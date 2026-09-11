#```python
import json
import logging
from datetime import datetime
from typing import Iterator

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

from src.models.traffic_event import TrafficEvent
from src.validators.traffic_validator import TrafficValidator
from src.kafka.dlq_producer import TrafficDLQProducer


logger = logging.getLogger(__name__)


class TrafficKafkaConsumer:
    """
    Kafka consumer for traffic.raw.

    Supports:
        - Manual Kafka offset commits
        - JSON deserialization
        - TrafficEvent reconstruction
        - Business validation
        - DLQ handling
        - Batch polling for high-throughput processing
        - Partition-aware offset commits

    The batch API is designed for the validation pipeline.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "traffic.raw",
        group_id: str = "traffic-consumer-group",
        auto_offset_reset: str = "earliest",
        validator: TrafficValidator | None = None,
        dlq_producer: TrafficDLQProducer | None = None,
        max_poll_records: int = 5000,
    ):
        self.topic = topic
        self.group_id = group_id
        self.max_poll_records = max_poll_records

        self.last_message = None

        self.validator = (
            validator
            if validator is not None
            else TrafficValidator()
        )

        self.dlq_producer = (
            dlq_producer
            if dlq_producer is not None
            else TrafficDLQProducer(
                bootstrap_servers=bootstrap_servers,
                topic="traffic.dlq",
            )
        )

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=None,
            enable_auto_commit=False,

            # Batch polling.
            max_poll_records=max_poll_records,

            # The validation process now works in batches.
            # This gives it enough time to process a batch
            # without triggering a consumer rebalance.
            max_poll_interval_ms=600000,

            # Kafka heartbeat/session configuration.
            session_timeout_ms=45000,
            heartbeat_interval_ms=15000,
            request_timeout_ms=60000,
        )

        logger.info(
            "Kafka consumer initialized | "
            "bootstrap_servers=%s | "
            "topic=%s | "
            "group_id=%s | "
            "max_poll_records=%s | "
            "auto_commit=%s",
            bootstrap_servers,
            topic,
            group_id,
            max_poll_records,
            False,
        )

    # ==============================================================
    # JSON DESERIALIZATION
    # ==============================================================

    @staticmethod
    def deserialize_message(
        value: bytes,
    ) -> dict:
        """
        Convert Kafka message bytes into a JSON dictionary.
        """

        decoded_value = value.decode("utf-8")

        message = json.loads(decoded_value)

        if not isinstance(message, dict):
            raise TypeError(
                "Kafka message must contain a JSON object"
            )

        return message

    # ==============================================================
    # EVENT RECONSTRUCTION
    # ==============================================================

    @staticmethod
    def message_to_event(
        message: dict,
    ) -> TrafficEvent:
        """
        Convert a Kafka JSON payload into TrafficEvent.
        """

        return TrafficEvent(
            event_id=message["event_id"],

            timestamp=datetime.fromisoformat(
                message["timestamp"]
            ),

            source_node=message["source_node"],

            destination_node=message["destination_node"],

            traffic_mbps=float(
                message["traffic_mbps"]
            ),

            demand_id=message["demand_id"],

            granularity=message["granularity"],

            unit=message["unit"],

            dataset=message["dataset"],

            source_format=message["source_format"],

            source_folder=message["source_folder"],

            source_file=message["source_file"],

            schema_version=message.get(
                "schema_version",
                "1.0",
            ),
        )

    # ==============================================================
    # VALIDATION
    # ==============================================================

    def validate_event(
        self,
        event: TrafficEvent,
    ) -> list[str]:
        """
        Validate a TrafficEvent.

        Returns:
            [] when valid.
            List of validation errors when invalid.
        """

        return self.validator.validate(event)

    # ==============================================================
    # DLQ
    # ==============================================================

    def send_to_dlq(
        self,
        *,
        message,
        error_type: str,
        error_message: str,
        validation_stage: str,
        event_id: str | None = None,
    ) -> None:
        """
        Send a failed message to traffic.dlq.

        The caller must only commit the raw offset after
        this method returns successfully.
        """

        future = self.dlq_producer.send(
            original_topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            payload=message.value,
            error_type=error_type,
            error_message=error_message,
            validation_stage=validation_stage,
            event_id=event_id,
        )

        metadata = future.get(
            timeout=10
        )

        logger.debug(
            "DLQ ACK RECEIVED | "
            "original_topic=%s | "
            "original_partition=%s | "
            "original_offset=%s | "
            "dlq_topic=%s | "
            "dlq_partition=%s | "
            "dlq_offset=%s",
            message.topic,
            message.partition,
            message.offset,
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

    # ==============================================================
    # PARTITION-AWARE COMMIT
    # ==============================================================

    def commit_offsets(
        self,
        offsets: dict[TopicPartition, int],
    ) -> None:
        """
        Commit successfully processed message offsets.

        Input:
            {
                TopicPartition(topic, partition): message_offset
            }

        Kafka stores the NEXT offset, therefore:

            processed offset 100
            committed offset 101
        """

        if not offsets:
            return

        commit_map = {}

        for topic_partition, message_offset in offsets.items():

            commit_map[topic_partition] = OffsetAndMetadata(
                message_offset + 1,
                None,
            )

        self.consumer.commit(
            offsets=commit_map
        )

        for topic_partition, message_offset in offsets.items():

            logger.debug(
                "KAFKA OFFSET COMMITTED | "
                "topic=%s | "
                "partition=%s | "
                "processed_offset=%s | "
                "committed_offset=%s",
                topic_partition.topic,
                topic_partition.partition,
                message_offset,
                message_offset + 1,
            )

    # ==============================================================
    # LEGACY SINGLE MESSAGE COMMIT
    # ==============================================================

    def commit_last_message(self) -> None:
        """
        Backward-compatible single-message commit.
        """

        if self.last_message is None:
            raise RuntimeError(
                "No Kafka message available for offset commit."
            )

        message = self.last_message

        topic_partition = TopicPartition(
            message.topic,
            message.partition,
        )

        self.commit_offsets(
            {
                topic_partition: message.offset
            }
        )

    # ==============================================================
    # BATCH CONSUMPTION
    # ==============================================================

    def consume_batch(
        self,
        batch_size: int = 500,
    ) -> list[tuple[TrafficEvent, object]]:
        """
        Poll and validate a batch of Kafka messages.

        Returns:
            List of:
                (TrafficEvent, original Kafka message)

        Valid messages:
            Returned to ValidationConsumer.
            They are NOT committed here.

        Invalid messages:
            Sent to traffic.dlq.
            Their raw offsets are committed only after DLQ ACK.

        This separation is important:

            raw
             |
             v
          validate
             |
        +----+----+
        |         |
       valid    invalid
        |         |
        v         v
      output     DLQ
        |         |
        v         v
       ACK       ACK
        |         |
        +----+----+
             |
             v
          commit
        """

        records = self.consumer.poll(
            timeout_ms=1000,
            max_records=batch_size,
        )

        if not records:
            return []

        valid_events = []

        for topic_partition, messages in records.items():

            for message in messages:

                self.last_message = message

                logger.debug(
                    "KAFKA MESSAGE RECEIVED | "
                    "topic=%s | "
                    "partition=%s | "
                    "offset=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                )

                # ==================================================
                # DESERIALIZATION
                # ==================================================

                try:

                    payload = self.deserialize_message(
                        message.value
                    )

                except Exception as exception:

                    logger.error(
                        "MESSAGE DESERIALIZATION FAILED | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                        exception,
                    )

                    try:

                        self.send_to_dlq(
                            message=message,
                            error_type=type(exception).__name__,
                            error_message=str(exception),
                            validation_stage="deserialization",
                        )

                        self.commit_offsets(
                            {
                                TopicPartition(
                                    message.topic,
                                    message.partition,
                                ): message.offset
                            }
                        )

                    except Exception:

                        logger.exception(
                            "DLQ PROCESSING FAILED | "
                            "topic=%s | "
                            "partition=%s | "
                            "offset=%s",
                            message.topic,
                            message.partition,
                            message.offset,
                        )

                    continue

                # ==================================================
                # EVENT RECONSTRUCTION
                # ==================================================

                try:

                    event = self.message_to_event(
                        payload
                    )

                except Exception as exception:

                    event_id = (
                        payload.get("event_id")
                        if isinstance(payload, dict)
                        else None
                    )

                    logger.error(
                        "EVENT RECONSTRUCTION FAILED | "
                        "event_id=%s | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        event_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        exception,
                    )

                    try:

                        self.send_to_dlq(
                            message=message,
                            error_type=type(exception).__name__,
                            error_message=str(exception),
                            validation_stage="event_reconstruction",
                            event_id=event_id,
                        )

                        self.commit_offsets(
                            {
                                TopicPartition(
                                    message.topic,
                                    message.partition,
                                ): message.offset
                            }
                        )

                    except Exception:

                        logger.exception(
                            "DLQ PROCESSING FAILED | "
                            "stage=event_reconstruction | "
                            "event_id=%s | "
                            "topic=%s | "
                            "partition=%s | "
                            "offset=%s",
                            event_id,
                            message.topic,
                            message.partition,
                            message.offset,
                        )

                    continue

                # ==================================================
                # BUSINESS VALIDATION
                # ==================================================

                try:

                    validation_errors = self.validate_event(
                        event
                    )

                except Exception as exception:

                    logger.error(
                        "VALIDATION EXECUTION FAILED | "
                        "event_id=%s | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        event.event_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        exception,
                    )

                    try:

                        self.send_to_dlq(
                            message=message,
                            error_type=type(exception).__name__,
                            error_message=str(exception),
                            validation_stage="validation",
                            event_id=event.event_id,
                        )

                        self.commit_offsets(
                            {
                                TopicPartition(
                                    message.topic,
                                    message.partition,
                                ): message.offset
                            }
                        )

                    except Exception:

                        logger.exception(
                            "DLQ PROCESSING FAILED | "
                            "stage=validation | "
                            "event_id=%s | "
                            "topic=%s | "
                            "partition=%s | "
                            "offset=%s",
                            event.event_id,
                            message.topic,
                            message.partition,
                            message.offset,
                        )

                    continue

                # ==================================================
                # INVALID EVENT
                # ==================================================

                if validation_errors:

                    error_message = "; ".join(
                        validation_errors
                    )

                    logger.warning(
                        "EVENT VALIDATION FAILED | "
                        "event_id=%s | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "errors=%s",
                        event.event_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        error_message,
                    )

                    try:

                        self.send_to_dlq(
                            message=message,
                            error_type="ValidationError",
                            error_message=error_message,
                            validation_stage="validation",
                            event_id=event.event_id,
                        )

                        self.commit_offsets(
                            {
                                TopicPartition(
                                    message.topic,
                                    message.partition,
                                ): message.offset
                            }
                        )

                    except Exception:

                        logger.exception(
                            "DLQ PROCESSING FAILED | "
                            "stage=validation | "
                            "event_id=%s | "
                            "topic=%s | "
                            "partition=%s | "
                            "offset=%s",
                            event.event_id,
                            message.topic,
                            message.partition,
                            message.offset,
                        )

                    continue

                # ==================================================
                # VALID EVENT
                # ==================================================

                valid_events.append(
                    (
                        event,
                        message,
                    )
                )

        return valid_events

    # ==============================================================
    # LEGACY SINGLE MESSAGE API
    # ==============================================================

    def consume(self) -> Iterator[TrafficEvent]:
        """
        Backward-compatible single-message API.

        Existing callers can continue using this method.
        """

        for message in self.consumer:

            self.last_message = message

            try:

                payload = self.deserialize_message(
                    message.value
                )

            except Exception as exception:

                try:

                    self.send_to_dlq(
                        message=message,
                        error_type=type(exception).__name__,
                        error_message=str(exception),
                        validation_stage="deserialization",
                    )

                    self.commit_last_message()

                except Exception:

                    logger.exception(
                        "DLQ processing failed."
                    )

                continue

            try:

                event = self.message_to_event(
                    payload
                )

            except Exception as exception:

                event_id = (
                    payload.get("event_id")
                    if isinstance(payload, dict)
                    else None
                )

                try:

                    self.send_to_dlq(
                        message=message,
                        error_type=type(exception).__name__,
                        error_message=str(exception),
                        validation_stage="event_reconstruction",
                        event_id=event_id,
                    )

                    self.commit_last_message()

                except Exception:

                    logger.exception(
                        "DLQ processing failed."
                    )

                continue

            try:

                validation_errors = self.validate_event(
                    event
                )

            except Exception as exception:

                try:

                    self.send_to_dlq(
                        message=message,
                        error_type=type(exception).__name__,
                        error_message=str(exception),
                        validation_stage="validation",
                        event_id=event.event_id,
                    )

                    self.commit_last_message()

                except Exception:

                    logger.exception(
                        "DLQ processing failed."
                    )

                continue

            if validation_errors:

                try:

                    self.send_to_dlq(
                        message=message,
                        error_type="ValidationError",
                        error_message="; ".join(
                            validation_errors
                        ),
                        validation_stage="validation",
                        event_id=event.event_id,
                    )

                    self.commit_last_message()

                except Exception:

                    logger.exception(
                        "DLQ processing failed."
                    )

                continue

            yield event

    # ==============================================================
    # CLOSE
    # ==============================================================

    def close(self) -> None:

        try:

            self.consumer.close()

        except Exception:

            logger.exception(
                "Error closing Kafka consumer."
            )

        try:

            self.dlq_producer.close()

        except Exception:

            logger.exception(
                "Error closing Kafka DLQ producer."
            )

        logger.info(
            "Kafka consumer and DLQ producer closed."
        )
#```
