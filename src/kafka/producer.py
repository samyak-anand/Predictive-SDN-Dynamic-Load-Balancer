import json
import logging

from kafka import KafkaProducer

from src.models.traffic_event import TrafficEvent


logger = logging.getLogger(__name__)


class TrafficKafkaProducer:
    """
    Kafka producer for publishing validated TrafficEvent objects.

    Responsibilities:
        - Serialize TrafficEvent to JSON
        - Publish events to Kafka
        - Handle delivery callbacks
        - Provide graceful shutdown

    Does NOT:
        - Parse XML/TXT
        - Validate events
        - Perform ML
        - Perform SDN decisions
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "traffic.raw",
    ):
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
            "Kafka producer initialized: %s",
            bootstrap_servers,
        )

    @staticmethod
    def event_to_dict(
        event: TrafficEvent
    ) -> dict:

        return {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),
            "source_node": event.source_node,
            "destination_node": event.destination_node,
            "traffic_mbps": event.traffic_mbps,
            "demand_id": event.demand_id,
            "granularity": event.granularity,
            "unit": event.unit,
            "dataset": event.dataset,
            "source_format": event.source_format,
            "source_folder": event.source_folder,
            "source_file": event.source_file,
            "schema_version": event.schema_version,
        }

    def _delivery_callback(
        self,
        event: TrafficEvent,
    ):
        def callback(
            metadata,
            exception,
        ):

            if exception is not None:

                logger.error(
                    "Kafka delivery failed | event_id=%s | error=%s",
                    event.event_id,
                    exception,
                )

                return

            logger.debug(
                "Kafka delivery successful | event_id=%s | topic=%s | partition=%s | offset=%s",
                event.event_id,
                metadata.topic,
                metadata.partition,
                metadata.offset,
            )

        return callback

    def send(
        self,
        event: TrafficEvent,
    ):
        message = self.event_to_dict(event)

        future = self.producer.send(
            self.topic,
            value=message,
        )

        future.add_callback(
            self._delivery_callback(event)
        )

        future.add_errback(
            lambda exception: logger.error(
                "Kafka send error | event_id=%s | error=%s",
                event.event_id,
                exception,
            )
        )

        return future

    def flush(self):
        logger.info("Flushing Kafka producer...")
        self.producer.flush()

    def close(self):
        logger.info("Closing Kafka producer...")
        self.producer.flush()
        self.producer.close()