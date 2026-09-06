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
    publish valid events to traffic.validated,
    and commit the input Kafka offset only after
    successful publication.

    Processing flow:

        traffic.raw
             |
             v
        TrafficKafkaConsumer
             |
             v
        Validation
             |
        +----+----+
        |         |
      VALID     INVALID
        |         |
        v         v
    traffic.   traffic.dlq
    validated
        |
        v
    Kafka ACK
        |
        v
    Commit traffic.raw offset
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
        self.failed = 0

    def run(self):
        """
        Consume events from traffic.raw.

        For every valid event:

            1. Publish to traffic.validated.
            2. Wait for Kafka acknowledgement.
            3. Commit the corresponding traffic.raw offset.

        The input offset is NEVER committed before the
        output Kafka message has been acknowledged.
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

        logger.info(
            "Consumer group    : %s",
            self.consumer.group_id,
        )

        logger.info(
            "Offset management : MANUAL",
        )

        try:

            for event in self.consumer.consume():

                self.processed += 1

                try:

                    # ==================================================
                    # STEP 1: PUBLISH VALIDATED EVENT
                    # ==================================================

                    future = self.producer.send(
                        event
                    )

                    # --------------------------------------------------
                    # Wait for Kafka acknowledgement.
                    #
                    # This makes sure the output message was actually
                    # accepted by Kafka before we commit the input
                    # offset.
                    # --------------------------------------------------

                    future.get(
                        timeout=10
                    )

                    logger.info(
                        "OUTPUT ACK RECEIVED | "
                        "event_id=%s | "
                        "output_topic=%s",
                        event.event_id,
                        self.producer.topic,
                    )

                    # ==================================================
                    # STEP 2: COMMIT INPUT OFFSET
                    # ==================================================

                    self.consumer.commit_last_message()

                    # ==================================================
                    # STEP 3: UPDATE SUCCESS COUNTER
                    # ==================================================

                    self.validated += 1

                    logger.info(
                        "VALIDATED EVENT | "
                        "event_id=%s | "
                        "status=SUCCESS",
                        event.event_id,
                    )

                except Exception as exception:

                    self.failed += 1

                    logger.exception(
                        "FAILED TO PROCESS EVENT | "
                        "event_id=%s | "
                        "error=%s",
                        event.event_id,
                        exception,
                    )

                    # --------------------------------------------------
                    # IMPORTANT:
                    #
                    # No Kafka offset is committed here.
                    #
                    # Therefore the message can be reprocessed after
                    # a consumer restart.
                    # --------------------------------------------------

                    continue

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

        logger.info(
            "Failed events          : %s",
            self.failed,
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