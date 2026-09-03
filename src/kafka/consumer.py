import json
import logging
from datetime import datetime

from kafka import KafkaConsumer

from src.models.traffic_event import TrafficEvent


logger = logging.getLogger(__name__)


class TrafficKafkaConsumer:
    """
    Kafka consumer for reading TrafficEvent objects
    from the traffic.raw topic.

    Responsibilities:
        - Connect to Kafka
        - Consume messages from Kafka
        - Deserialize JSON messages
        - Convert messages into TrafficEvent objects
        - Log successful message consumption
        - Handle malformed messages
        - Gracefully shut down

    Does NOT:
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
    ):
        self.topic = topic
        self.group_id = group_id

        self.consumer = KafkaConsumer(
            topic,

            bootstrap_servers=bootstrap_servers,

            group_id=group_id,

            auto_offset_reset=auto_offset_reset,

            enable_auto_commit=True,

            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),

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
    def message_to_event(
        message: dict,
    ) -> TrafficEvent:
        """
        Convert a Kafka JSON message into a TrafficEvent.
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

    def consume(self):
        """
        Consume messages from Kafka.

        Yields:
            TrafficEvent objects.
        """

        logger.info(
            "Starting Kafka consumption | topic=%s",
            self.topic,
        )

        try:

            for message in self.consumer:

                try:

                    event = self.message_to_event(
                        message.value
                    )

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

                except (
                    KeyError,
                    TypeError,
                    ValueError,
                    json.JSONDecodeError,
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

        except KeyboardInterrupt:

            logger.info(
                "Kafka consumer interrupted."
            )

        finally:

            self.close()

    def close(self):
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