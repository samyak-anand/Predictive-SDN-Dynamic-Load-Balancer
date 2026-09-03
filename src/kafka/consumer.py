import json
import logging
from datetime import datetime
from typing import Iterator

from kafka import KafkaConsumer

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
        +---------+
        |         |
      VALID     INVALID
        |         |
        v         v
    TrafficEvent  traffic.dlq
        |
        v
    TrafficValidator
        |
      +---+---+
      |       |
    VALID   INVALID
      |       |
      v       v
    yield   traffic.dlq
    event

    Responsibilities:
        - Connect to Kafka
        - Consume raw Kafka messages
        - Deserialize JSON
        - Reconstruct TrafficEvent
        - Validate TrafficEvent
        - Yield valid events
        - Route invalid messages to DLQ
        - Log processing failures
        - Gracefully close

    Does NOT:
        - Parse XML/TXT files
        - Perform feature engineering
        - Perform ML prediction
        - Perform SDN decisions
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

            # Consume raw bytes.
            #
            # JSON deserialization is handled manually so that
            # malformed messages can be routed to the DLQ.
            value_deserializer=None,

            # Keep the current behavior for the first integration
            # test. Manual offset commits can be introduced before
            # large-scale production ingestion.
            enable_auto_commit=True,

            # Prevent the consumer from blocking indefinitely
            # during tests or controlled shutdown.
            consumer_timeout_ms=1000,
        )

        logger.info(
            "Kafka consumer initialized | "
            "bootstrap_servers=%s | "
            "topic=%s | "
            "group_id=%s",
            bootstrap_servers,
            topic,
            group_id,
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

        The original Kafka metadata and payload are preserved
        by TrafficDLQProducer.
        """

        self.dlq_producer.send(
            original_topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            payload=message.value,
            error_type=error_type,
            error_message=error_message,
            validation_stage=validation_stage,
            event_id=event_id,
        )

    # ==============================================================
    # CONSUME
    # ==============================================================

    def consume(
        self,
    ) -> Iterator[TrafficEvent]:
        """
        Consume and validate Kafka messages.

        Processing:

            Kafka message
                |
                v
            Deserialize JSON
                |
                +--------------------+
                |                    |
              VALID                INVALID
                |                    |
                v                    v
          Build TrafficEvent     traffic.dlq
                |
                v
          Validate TrafficEvent
                |
              +---+---+
              |       |
            VALID   INVALID
              |       |
              v       v
            yield   traffic.dlq
            event

        Valid events continue downstream.

        Invalid messages are published to the DLQ and skipped.
        """

        logger.info(
            "Starting Kafka consumption | "
            "topic=%s | "
            "group_id=%s",
            self.topic,
            self.group_id,
        )

        try:

            for message in self.consumer:

                # ==================================================
                # STEP 1: DESERIALIZE JSON
                # ==================================================

                try:

                    payload = self.deserialize_message(
                        message.value
                    )

                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    TypeError,
                ) as exception:

                    logger.error(
                        "INVALID KAFKA MESSAGE | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                        exception,
                    )

                    self.send_to_dlq(
                        message=message,
                        error_type=type(
                            exception
                        ).__name__,
                        error_message=str(
                            exception
                        ),
                        validation_stage=(
                            "deserialization"
                        ),
                    )

                    continue

                # ==================================================
                # STEP 2: RECONSTRUCT TRAFFIC EVENT
                # ==================================================

                try:

                    event = self.message_to_event(
                        payload
                    )

                except (
                    KeyError,
                    TypeError,
                    ValueError,
                ) as exception:

                    logger.error(
                        "INVALID EVENT STRUCTURE | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "error=%s",
                        message.topic,
                        message.partition,
                        message.offset,
                        exception,
                    )

                    self.send_to_dlq(
                        message=message,
                        error_type=type(
                            exception
                        ).__name__,
                        error_message=str(
                            exception
                        ),
                        validation_stage=(
                            "event_reconstruction"
                        ),
                        event_id=payload.get(
                            "event_id"
                        ),
                    )

                    continue

                # ==================================================
                # STEP 3: VALIDATE TRAFFIC EVENT
                # ==================================================

                errors = self.validate_event(
                    event
                )

                if errors:

                    logger.error(
                        "INVALID EVENT | "
                        "event_id=%s | "
                        "topic=%s | "
                        "partition=%s | "
                        "offset=%s | "
                        "errors=%s",
                        event.event_id,
                        message.topic,
                        message.partition,
                        message.offset,
                        errors,
                    )

                    self.send_to_dlq(
                        message=message,
                        error_type=(
                            "TrafficValidationError"
                        ),
                        error_message="; ".join(
                            errors
                        ),
                        validation_stage=(
                            "traffic_validation"
                        ),
                        event_id=event.event_id,
                    )

                    continue

                # ==================================================
                # STEP 4: VALID EVENT
                # ==================================================

                logger.info(
                    "ENTRY RECEIVED | "
                    "event_id=%s | "
                    "topic=%s | "
                    "partition=%s | "
                    "offset=%s",
                    event.event_id,
                    message.topic,
                    message.partition,
                    message.offset,
                )

                # Valid event continues downstream.
                yield event

        except KeyboardInterrupt:

            logger.info(
                "Kafka consumer interrupted."
            )

        finally:

            self.close()

    # ==============================================================
    # CLOSE
    # ==============================================================

    def close(
        self,
    ) -> None:
        """
        Gracefully close the Kafka consumer
        and DLQ producer.
        """

        logger.info(
            "Closing Kafka consumer..."
        )

        self.consumer.close()

        logger.info(
            "Closing DLQ producer..."
        )

        self.dlq_producer.close()

        logger.info(
            "Kafka consumer and DLQ producer closed."
        )