import logging

from src.kafka.consumer import TrafficKafkaConsumer
from src.kafka.producer import TrafficKafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


class ValidationConsumer:
    """
    Consume events from traffic.raw, validate them,
    and publish valid events to traffic.validated.

    Flow:

        traffic.raw
             |
             v
        TrafficKafkaConsumer
             |
        +----+----+
        |         |
      VALID     INVALID
        |         |
        v         v
    traffic.   traffic.dlq
    validated
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        raw_topic: str = "traffic.raw",
        validated_topic: str = "traffic.validated",
        group_id: str = "traffic-validation-consumer",
    ):
        self.producer = TrafficKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            topic=validated_topic,
        )

        self.consumer = TrafficKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            topic=raw_topic,
            group_id=group_id,
            auto_offset_reset="earliest",
        )

        self.processed = 0
        self.validated = 0

    def run(self):
        """
        Consume events from traffic.raw and publish
        valid events to traffic.validated.
        """

        logger.info("=" * 70)
        logger.info("Starting Validation Consumer")
        logger.info("=" * 70)

        logger.info(
            "Input topic       : %s",
            self.consumer.topic,
        )

        logger.info(
            "Output topic      : %s",
            self.producer.topic,
        )

        try:
            for event in self.consumer.consume():

                self.processed += 1

                try:
                    self.producer.send(event)

                    self.validated += 1

                    logger.info(
                        "VALIDATED EVENT | "
                        "event_id=%s | "
                        "output_topic=%s",
                        event.event_id,
                        self.producer.topic,
                    )

                except Exception as exception:

                    logger.exception(
                        "FAILED TO PUBLISH VALIDATED EVENT | "
                        "event_id=%s | "
                        "error=%s",
                        event.event_id,
                        exception,
                    )

            self.producer.flush()

        except KeyboardInterrupt:

            logger.info(
                "Validation consumer interrupted."
            )

        finally:

            self.close()

        logger.info("=" * 70)
        logger.info("Validation Consumer Summary")
        logger.info("=" * 70)
        logger.info(
            "Processed valid events : %s",
            self.processed,
        )
        logger.info(
            "Published validated    : %s",
            self.validated,
        )
        logger.info("=" * 70)

    def close(self):
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
            self.producer.close()
        except Exception:
            logger.exception(
                "Error closing Kafka producer."
            )


if __name__ == "__main__":

    validation_consumer = ValidationConsumer()

    validation_consumer.run()