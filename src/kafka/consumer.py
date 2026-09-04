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
    Kafka consumer for reading TrafficEvent objects
    from the traffic.raw topic.

    Processing flow:

        Kafka
          |
          v
        Raw Kafka message
          |
          v
        JSON deserialization
          |
        +----------------+
        |                |
      SUCCESS          FAILURE
        |                |
        v                v
    TrafficEvent       traffic.dlq
        |                |
        v                v
    Validation        Kafka ACK
        |                |
      +---+---+          |
      |       |          |
    VALID   INVALID      |
      |       |          |
      v       v          |
    yield   traffic.dlq  |
    event       |        |
                v        |
            Kafka ACK    |
                |        |
                +----+---+
                     |
                     v
              Commit raw offset

    Offset management:

        Consume message
             |
             v
        Successful downstream
        processing / DLQ ACK
             |
             v
        commit_last_message()

    Kafka offsets are committed manually.

    The offset committed for message N is N + 1 because
    Kafka stores the NEXT offset to consume.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "traffic.raw",
        group_id: str = "traffic-consumer-group",
        auto_offset_reset: str = "earliest",
        validator: TrafficValidator | None = None,
        dlq_producer: TrafficDLQProducer | None = None,
    ):
        """
        Initialize the Kafka consumer.

        Args:
            bootstrap_servers:
                Kafka broker address.

            topic:
                Kafka topic to consume from.

            group_id:
                Kafka consumer group identifier.

            auto_offset_reset:
                Offset behavior when no committed offset exists.

                "earliest":
                    Read existing messages from the beginning.

                "latest":
                    Read only newly arriving messages.

            validator:
                Optional TrafficValidator instance.

            dlq_producer:
                Optional TrafficDLQProducer instance.
        """

        self.topic = topic
        self.group_id = group_id

        # ----------------------------------------------------------
        # Track the Kafka message currently being processed
        # ----------------------------------------------------------

        self.last_message = None

        # ----------------------------------------------------------
        # Traffic validator
        # ----------------------------------------------------------

        self.validator = (
            validator
            if validator is not None
            else TrafficValidator()
        )

        # ----------------------------------------------------------
        # DLQ producer
        # ----------------------------------------------------------

        self.dlq_producer = (
            dlq_producer
            if dlq_producer is not None
            else TrafficDLQProducer(
                bootstrap_servers=bootstrap_servers,
                topic="traffic.dlq",
            )
        )

        # ----------------------------------------------------------
        # Kafka consumer
        # ----------------------------------------------------------

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,

            # Consume raw bytes so malformed messages can
            # be explicitly handled and sent to the DLQ.
            value_deserializer=None,

            # Manual offset management.
            enable_auto_commit=False,

            # Prevent indefinite blocking during controlled
            # test execution and shutdown.
            consumer_timeout_ms=1000,
        )

        logger.info(
            "Kafka consumer initialized | "
            "bootstrap_servers=%s | "
            "topic=%s | "
            "group_id=%s | "
            "auto_commit=%s",
            bootstrap_servers,
            topic,
            group_id,
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
        Deserialize raw Kafka message bytes into a dictionary.

        Raises:
            UnicodeDecodeError:
                If the message is not valid UTF-8.

            json.JSONDecodeError:
                If the message is not valid JSON.

            TypeError:
                If the decoded JSON is not a dictionary.
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
        Convert a Kafka JSON dictionary into a TrafficEvent.

        Raises:
            KeyError:
                Required field is missing.

            TypeError:
                Field has an unexpected type.

            ValueError:
                Timestamp or numeric value is invalid.
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
        Validate a reconstructed TrafficEvent.

        Returns:
            Empty list if the event is valid.
            Otherwise, a list of validation errors.
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
        Send a failed Kafka message to the Dead Letter Queue.

        IMPORTANT:

        The raw Kafka offset must NOT be committed until
        Kafka acknowledges the DLQ message.

        Processing sequence:

            raw message
                 |
                 v
            publish DLQ
                 |
                 v
             Kafka ACK
                 |
                 v
          return successfully

        If the DLQ publish fails, future.get() raises an
        exception and the caller must NOT commit the raw offset.
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

        # ----------------------------------------------------------
        # WAIT FOR KAFKA ACK
        # ----------------------------------------------------------

        metadata = future.get(
            timeout=10
        )

        logger.info(
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
    # OFFSET COMMIT
    # ==============================================================

    def commit_last_message(self) -> None:
        """
        Commit the offset of the last successfully processed
        Kafka message.

        Kafka commits the NEXT offset to consume.

        Example:

            Processed offset = 10
            Committed offset = 11

        This method must only be called AFTER downstream
        processing has completed successfully.
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

        next_offset = message.offset + 1

        offsets = {
            topic_partition: OffsetAndMetadata(
                next_offset,
                None,
            )
        }

        self.consumer.commit(
            offsets=offsets
        )

        logger.info(
            "KAFKA OFFSET COMMITTED | "
            "topic=%s | "
            "partition=%s | "
            "processed_offset=%s | "
            "committed_offset=%s",
            message.topic,
            message.partition,
            message.offset,
            next_offset,
        )

    # ==============================================================
    # CONSUME
    # ==============================================================

    def consume(self) -> Iterator[TrafficEvent]:
        """
        Consume, deserialize, reconstruct and validate Kafka messages.

        Valid messages:
            - Yield TrafficEvent to downstream consumer.
            - Downstream component is responsible for committing
              the raw Kafka offset.

        Invalid messages:
            - Publish the original Kafka message to the DLQ.
            - Wait for Kafka DLQ acknowledgement.
            - Commit the raw Kafka offset ONLY after DLQ ACK.
            - Do not yield the invalid event.

        This prevents poison messages from repeatedly blocking
        the consumer after they have been safely captured in the DLQ.
        """

        for message in self.consumer:

            # ------------------------------------------------------
            # Store current Kafka message
            # ------------------------------------------------------

            self.last_message = message

            logger.info(
                "KAFKA MESSAGE RECEIVED | "
                "topic=%s | "
                "partition=%s | "
                "offset=%s",
                message.topic,
                message.partition,
                message.offset,
            )

            # ======================================================
            # STEP 1: DESERIALIZATION
            # ======================================================

            try:

                payload = self.deserialize_message(
                    message.value
                )

            except Exception as exception:

                logger.exception(
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

                    # ----------------------------------------------
                    # Publish malformed message to DLQ.
                    # ----------------------------------------------

                    self.send_to_dlq(
                        message=message,
                        error_type=type(exception).__name__,
                        error_message=str(exception),
                        validation_stage="deserialization",
                    )

                    # ----------------------------------------------
                    # DLQ ACK received.
                    #
                    # Safe to commit the raw offset.
                    # ----------------------------------------------

                    self.commit_last_message()

                    logger.info(
                        "INVALID MESSAGE HANDLED | "
                        "stage=deserialization | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                    )

                except Exception as dlq_exception:

                    logger.exception(
                        "DLQ PROCESSING FAILED | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                        dlq_exception,
                    )

                    # ----------------------------------------------
                    # IMPORTANT:
                    #
                    # Do NOT commit the raw offset.
                    #
                    # The message remains replayable.
                    # ----------------------------------------------

                continue

            # ======================================================
            # STEP 2: EVENT RECONSTRUCTION
            # ======================================================

            try:

                event = self.message_to_event(
                    payload
                )

            except Exception as exception:

                event_id = payload.get(
                    "event_id"
                ) if isinstance(
                    payload,
                    dict
                ) else None

                logger.exception(
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

                    self.commit_last_message()

                    logger.info(
                        "INVALID MESSAGE HANDLED | "
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

                except Exception as dlq_exception:

                    logger.exception(
                        "DLQ PROCESSING FAILED | "
                        "stage=event_reconstruction | "
                        "event_id=%s | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        event_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        dlq_exception,
                    )

                continue

            # ======================================================
            # STEP 3: BUSINESS VALIDATION
            # ======================================================

            try:

                validation_errors = self.validate_event(
                    event
                )

            except Exception as exception:

                logger.exception(
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

                    self.commit_last_message()

                    logger.info(
                        "INVALID MESSAGE HANDLED | "
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

                except Exception as dlq_exception:

                    logger.exception(
                        "DLQ PROCESSING FAILED | "
                        "stage=validation | "
                        "event_id=%s | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        event.event_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        dlq_exception,
                    )

                continue

            # ======================================================
            # STEP 4: VALIDATION RESULT
            # ======================================================

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

                    self.commit_last_message()

                    logger.info(
                        "INVALID MESSAGE HANDLED | "
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

                except Exception as dlq_exception:

                    logger.exception(
                        "DLQ PROCESSING FAILED | "
                        "stage=validation | "
                        "event_id=%s | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        event.event_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        dlq_exception,
                    )

                continue

            # ======================================================
            # STEP 5: VALID EVENT
            # ======================================================

            logger.info(
                "EVENT VALIDATION SUCCESS | "
                "event_id=%s | "
                "topic=%s | "
                "partition=%s | "
                "offset=%s",
                event.event_id,
                message.topic,
                message.partition,
                message.offset,
            )

            # ------------------------------------------------------
            # Yield valid event to downstream processing.
            #
            # The downstream component is responsible for:
            #
            #     publish to traffic.validated
            #                 ↓
            #             Kafka ACK
            #                 ↓
            #         commit_last_message()
            #
            # ------------------------------------------------------

            yield event

    # ==============================================================
    # CLOSE
    # ==============================================================

    def close(self) -> None:
        """
        Gracefully close Kafka resources.
        """

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
                "Error closing DLQ producer."
            )

        logger.info(
            "Kafka consumer and DLQ producer closed."
        )