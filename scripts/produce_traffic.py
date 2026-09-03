import argparse
import logging
from pathlib import Path

from src.kafka.producer import TrafficKafkaProducer
from src.loaders.dataset_loader import DatasetLoader
from src.pipelines.validation_pipeline import ValidationPipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


DEFAULT_DATASET_PATH = (
    "/home/samyak/PycharmProjects/"
    "Predictive-SDN-Dynamic-Load-Balancer_data/"
    "directed-abilene-zhang-5min-over-6months-ALL-native"
)


def produce_traffic(
    dataset_path: str,
    bootstrap_servers: str,
    topic: str,
    limit: int | None = None,
):
    """
    Stream validated Abilene traffic events into Kafka.

    Pipeline:

        DatasetLoader
            ↓
        ValidationPipeline
            ↓
        TrafficKafkaProducer
            ↓
        Kafka topic
    """

    loader = DatasetLoader(
        dataset_name="abilene"
    )

    validation_pipeline = ValidationPipeline()

    producer = TrafficKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )

    produced = 0

    try:

        events = loader.load(
            Path(dataset_path)
        )

        valid_events = validation_pipeline.process_stream(
            events
        )

        for event in valid_events:

            producer.send(event)

            produced += 1

            if produced % 10 == 0:
                logger.info(
                    "Produced %d events",
                    produced,
                )

            if limit is not None and produced >= limit:
                break

        producer.flush()

        logger.info(
            "Kafka ingestion completed | produced=%d",
            produced,
        )

    finally:

        producer.close()


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Stream Abilene traffic events "
            "into Kafka."
        )
    )

    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_PATH,
        help="Path to the Abilene dataset",
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap server",
    )

    parser.add_argument(
        "--topic",
        default="traffic.raw",
        help="Kafka topic",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of events to publish",
    )

    args = parser.parse_args()

    produce_traffic(
        dataset_path=args.dataset,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()