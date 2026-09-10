import argparse
import logging
import time
from pathlib import Path

from src.kafka.producer import TrafficKafkaProducer
from src.loaders.dataset_loader import DatasetLoader


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("dataset_producer")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stream Abilene dataset events into Kafka."
    )

    parser.add_argument(
        "--data-root",
        required=True,
        help="Path to the dataset directory",
    )

    parser.add_argument(
        "--format",
        choices=["native", "xml"],
        default="native",
        help="Dataset representation to ingest",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum events to publish. 0 = all events.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Log progress every N events.",
    )

    return parser.parse_args()


def run():
    args = parse_args()

    data_root = Path(args.data_root)

    if args.format == "native":
        dataset_dir = (
            data_root
            / "directed-abilene-zhang-5min-over-6months-ALL-native"
        )
    else:
        dataset_dir = (
            data_root
            / "directed-abilene-zhang-5min-over-6months-ALL"
        )

    logger.info("=" * 70)
    logger.info("Starting Dataset → Kafka ingestion")
    logger.info("=" * 70)
    logger.info("Dataset root : %s", data_root)
    logger.info("Format       : %s", args.format)
    logger.info("Dataset path : %s", dataset_dir)
    logger.info("Event limit  : %s", args.limit or "ALL")
    logger.info("Kafka topic  : traffic.raw")
    logger.info("=" * 70)

    if not dataset_dir.exists():
        raise FileNotFoundError(
            f"Dataset directory does not exist: {dataset_dir}"
        )

    loader = DatasetLoader(dataset_name="abilene")

    producer = TrafficKafkaProducer(
        bootstrap_servers="localhost:9092",
        topic="traffic.raw",
    )

    total = 0
    start_time = time.time()

    try:
        for event in loader.load(dataset_dir):

            producer.send(event)

            total += 1

            if total % args.progress_every == 0:
                elapsed = time.time() - start_time
                rate = total / elapsed if elapsed > 0 else 0

                logger.info(
                    "INGESTION PROGRESS | "
                    "events=%d | rate=%.2f events/sec | "
                    "latest=%s",
                    total,
                    rate,
                    event.event_id,
                )

            if args.limit > 0 and total >= args.limit:
                logger.info(
                    "Event limit reached: %d",
                    args.limit,
                )
                break

        logger.info(
            "Waiting for Kafka to acknowledge pending messages..."
        )

        producer.flush()

        elapsed = time.time() - start_time
        rate = total / elapsed if elapsed > 0 else 0

        logger.info("=" * 70)
        logger.info("Dataset ingestion completed")
        logger.info("Total events published : %d", total)
        logger.info("Elapsed time           : %.2f sec", elapsed)
        logger.info("Average rate           : %.2f events/sec", rate)
        logger.info("=" * 70)

    except KeyboardInterrupt:
        logger.warning("Ingestion interrupted by user.")

        producer.flush()

    finally:
        producer.close()


if __name__ == "__main__":
    run()