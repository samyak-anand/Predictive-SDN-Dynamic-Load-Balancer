import json
import logging
from datetime import datetime
from typing import Iterator

from kafka import KafkaConsumer

from src.models.traffic_event import TrafficEvent
from src.validators.traffic_validator import TrafficValidator


logger = logging.getLogger(__name__)


class TrafficKafkaConsumer:
    """
    Kafka consumer for reading TrafficEvent objects
    from the traffic.raw topic.

    Responsibilities:
        - Connect to Kafka
        - Consume messages from Kafka
        - Deserialize JSON messages
        - Convert JSON messages into TrafficEvent objects
        - Validate reconstructed TrafficEvent objects
        - Yield only valid events
        - Log consumption and validation failures
        - Gracefully shut down

    Does NOT:
        - Parse XML/TXT files
        - Publish messages to Kafka
        - Perform feature engineering
        - Perform ML prediction
        - Perform SDN decisions

    Processing flow:

        Kafka
          |
          v
        JSON message
          |
          v
        TrafficEvent
          |
          v
        TrafficValidator
          |
        +---------+
        |         |
      valid     invalid
        |         |
        v         v
      yield     reject
      event
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "traffic.raw",
        group_id: str = "traffic-consumer-group",
        auto_offset_reset: str = "earliest",
        validator: TrafficValidator | None = None,
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
                Where to start when no committed offset exists.
                Usually:
                    - "earliest" for testing/replay
                    - "latest" for live streaming

            validator:
                Optional TrafficValidator instance.
        """

        self.topic = topic
        self.group_id = group_id

        self.validator = (
            validator
            if validator is not None
            else TrafficValidator()
        )

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=self._deserialize_message,
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

    @staticmethod
    def _deserialize_message(
        value: bytes,
    ) -> dict:
        """
        Deserialize a Kafka message from JSON bytes
        into a Python dictionary.

        JSON errors are intentionally allowed to propagate
        so that the consumer can handle malformed messages
        explicitly.
        """

        return json.loads(
            value.decode("utf-8")
        )

    @staticmethod
    def message_to_event(
        message: dict,
    ) -> TrafficEvent:
        """
        Convert a Kafka JSON message into a TrafficEvent.

        Raises:
            KeyError:
                Required field is missing.

            TypeError:
                Message has an unexpected type.

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

    def validate_event(
        self,
        event: TrafficEvent,
    ) -> list[str]:
        """
        Validate a reconstructed TrafficEvent.

        Returns:
            List of validation errors.
            Empty list means the event is valid.
        """

        return self.validator.validate(event)

    def consume(self) -> Iterator[TrafficEvent]:
        """
        Consume, deserialize, reconstruct, and validate
        Kafka messages.

        Only valid TrafficEvent objects are yielded.

        Invalid messages are logged and skipped.

        Yields:
            Valid TrafficEvent objects.
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

                try:
                    event = self.message_to_event(
                        message.value
                    )

                except (
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    KeyError,
                    TypeError,
                    ValueError,
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

                    continue

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

                    continue

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

                yield event

        except KeyboardInterrupt:

            logger.info(
                "Kafka consumer interrupted."
            )

        finally:

            self.close()

    def close(self) -> None:
        """
        Gracefully close the Kafka consumer.
        """

        logger.info(
            "Closing Kafka consumer..."
        )

        self.consumer.close()

        logger.info(
            "Kafka consumer closed."
        )