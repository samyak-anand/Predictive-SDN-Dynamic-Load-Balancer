#```python
import argparse
import logging
import time

from src.loaders.dataset_loader import DatasetLoader
from src.pipelines.validation_pipeline import ValidationPipeline
from src.kafka.producer import TrafficKafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


DEFAULT_DATASET = (
    "/home/samyak/PycharmProjects/"
    "Predictive-SDN-Dynamic-Load-Balancer_data/"
    "directed-abilene-zhang-5min-over-6months-ALL-native"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream validated traffic events to Kafka."
    )

    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help="Path to the traffic dataset.",
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap server.",
    )

    parser.add_argument(
        "--topic",
        default="traffic.raw",
        help="Kafka topic.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of events to produce.",
    )

    return parser.parse_args()


def print_summary(
    events_read,
    events_produced,
    events_failed,
    elapsed_time,
):
    throughput = (
        events_produced / elapsed_time
        if elapsed_time > 0
        else 0
    )

    logger.info("=" * 60)
    logger.info("Kafka Ingestion Summary")
    logger.info("=" * 60)
    logger.info("Input events       : %d", events_read)
    logger.info("Produced events    : %d", events_produced)
    logger.info("Failed events      : %d", events_failed)
    logger.info("Elapsed time       : %.2f sec", elapsed_time)
    logger.info("Throughput         : %.2f events/sec", throughput)
    logger.info("=" * 60)


def main():
    args = parse_args()

    logger.info("Starting Kafka ingestion...")
    logger.info("Dataset: %s", args.dataset)
    logger.info("Kafka: %s", args.bootstrap_servers)
    logger.info("Topic: %s", args.topic)

    if args.limit is not None:
        logger.info("Limit: %d events", args.limit)
    else:
        logger.info("Limit: None - full dataset")

    start_time = time.perf_counter()

    events_read = 0
    events_produced = 0
    events_failed = 0

    loader = DatasetLoader(
        dataset_name="abilene"
    )

    pipeline = ValidationPipeline()

    producer = TrafficKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )

    try:
        # DatasetLoader produces a streaming iterator.
        events = loader.load(args.dataset)

        # ValidationPipeline.process_stream()
        # expects an iterable and yields only
        # valid + unique events.
        validated_events = pipeline.process_stream(events)

        for event in validated_events:

            events_read += 1

            try:
                producer.send(event)

                events_produced += 1

            except Exception:
                events_failed += 1

                logger.exception(
                    "Failed to produce event | event_id=%s",
                    event.event_id,
                )

                continue

            if (
                args.limit is not None
                and events_produced >= args.limit
            ):
                break

            if events_produced % 1000 == 0:
                elapsed = time.perf_counter() - start_time

                throughput = (
                    events_produced / elapsed
                    if elapsed > 0
                    else 0
                )

                logger.info(
                    "Progress | events=%d | "
                    "failed=%d | "
                    "throughput=%.2f events/sec",
                    events_produced,
                    events_failed,
                    throughput,
                )

        # Make sure every outstanding Kafka message
        # has been acknowledged by the broker.
        producer.flush()

        logger.info(
            "Kafka ingestion completed | produced=%d",
            events_produced,
        )

    except KeyboardInterrupt:
        logger.warning(
            "Kafka ingestion interrupted by user."
        )

    except Exception:
        logger.exception(
            "Kafka ingestion failed."
        )
        raise

    finally:
        producer.close()

        elapsed_time = time.perf_counter() - start_time

        print_summary(
            events_read=events_read,
            events_produced=events_produced,
            events_failed=events_failed,
            elapsed_time=elapsed_time,
        )


if __name__ == "__main__":
    main()
#```
