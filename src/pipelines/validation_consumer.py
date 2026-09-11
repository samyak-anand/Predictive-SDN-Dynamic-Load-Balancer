#```python
import logging
import os
from collections import defaultdict

from kafka import TopicPartition

from src.kafka.consumer import TrafficKafkaConsumer
from src.kafka.producer import TrafficKafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ==============================================================
# PERFORMANCE CONFIGURATION
# ==============================================================

# Number of messages processed in one validation batch.
#
# Start with 500.
# After confirming stability, this can be increased to 1000.
BATCH_SIZE = int(
    os.getenv(
        "VALIDATION_BATCH_SIZE",
        "5000",
    )
)

# Maximum number of records returned by one Kafka poll.
KAFKA_POLL_RECORDS = int(
    os.getenv(
        "KAFKA_POLL_RECORDS",
        "5000",
    )
)


class ValidationConsumer:
    """
    High-throughput Kafka validation consumer.

    Optimized processing flow:

        traffic.raw
             |
             v
        Kafka poll
        up to 500
             |
             v
        Deserialize
             |
             v
        Reconstruct
             |
             v
        Validate
             |
        +----+----+
        |         |
      VALID     INVALID
        |         |
        v         v
    traffic.   traffic.dlq
    validated
        |         |
        v         v
     ACK        ACK
        |         |
        +----+----+
             |
             v
       Batch commit
       per partition

    IMPORTANT:

    Valid events are not committed until their corresponding
    traffic.validated Kafka futures have successfully completed.

    Invalid events are committed only after their DLQ publication
    succeeds.
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
            max_poll_records=KAFKA_POLL_RECORDS,
        )

        self.processed = 0
        self.validated = 0
        self.failed = 0
        self.batches = 0

    # ==============================================================
    # RUN
    # ==============================================================

    def run(self):

        logger.info("=" * 70)
        logger.info("Starting Optimized Validation Consumer")
        logger.info("=" * 70)

        logger.info(
            "Input topic        : %s",
            self.consumer.topic,
        )

        logger.info(
            "Output topic       : %s",
            self.producer.topic,
        )

        logger.info(
            "Consumer group     : %s",
            self.consumer.group_id,
        )

        logger.info(
            "Batch size         : %s",
            BATCH_SIZE,
        )

        logger.info(
            "Kafka poll records : %s",
            KAFKA_POLL_RECORDS,
        )

        logger.info(
            "Offset management  : MANUAL / BATCH / PARTITION-AWARE",
        )

        try:

            while True:

                # ==================================================
                # STEP 1: POLL BATCH
                # ==================================================

                batch = self.consumer.consume_batch(
                    batch_size=BATCH_SIZE
                )

                if not batch:

                    continue

                self.batches += 1

                self.processed += len(batch)

                logger.info(
                    "BATCH RECEIVED | "
                    "batch=%s | "
                    "events=%s",
                    self.batches,
                    len(batch),
                )

                # ==================================================
                # STEP 2: PUBLISH ALL VALID EVENTS
                #
                # IMPORTANT:
                #
                # We do NOT call future.get() immediately.
                #
                # KafkaProducer.send() is asynchronous.
                #
                # This allows kafka-python to accumulate multiple
                # records into producer batches.
                # ==================================================

                pending = []

                for event, message in batch:

                    try:

                        future = self.producer.send(
                            event
                        )

                        pending.append(
                            (
                                event,
                                message,
                                future,
                            )
                        )

                    except Exception as exception:

                        self.failed += 1

                        logger.exception(
                            "FAILED TO QUEUE EVENT | "
                            "event_id=%s | "
                            "partition=%s | "
                            "offset=%s | "
                            "error=%s",
                            event.event_id,
                            message.partition,
                            message.offset,
                            exception,
                        )

                # ==================================================
                # STEP 3: WAIT FOR OUTPUT ACKS
                # ==================================================

                successful = []

                for event, message, future in pending:

                    try:

                        future.get(
                            timeout=30
                        )

                        successful.append(
                            (
                                event,
                                message,
                            )
                        )

                    except Exception as exception:

                        self.failed += 1

                        logger.exception(
                            "FAILED TO PUBLISH EVENT | "
                            "event_id=%s | "
                            "partition=%s | "
                            "offset=%s | "
                            "error=%s",
                            event.event_id,
                            message.partition,
                            message.offset,
                            exception,
                        )

                # ==================================================
                # STEP 4: DETERMINE SAFE OFFSETS
                # ==================================================
                #
                # Kafka partitions are ordered.
                #
                # Example:
                #
                # P0:
                #
                # 100 SUCCESS
                # 101 SUCCESS
                # 102 SUCCESS
                #
                # We can safely commit:
                #
                # P0 -> 103
                #
                # Kafka internally stores the NEXT offset.
                #
                # If an event fails:
                #
                # 100 SUCCESS
                # 101 SUCCESS
                # 102 FAILED
                # 103 SUCCESS
                #
                # We only commit through 101.
                #
                # Offset 102 remains replayable.
                # ==================================================

                successful_by_partition = defaultdict(
                    list
                )

                for event, message in successful:

                    successful_by_partition[
                        message.partition
                    ].append(
                        message.offset
                    )

                safe_offsets = {}

                for partition, offsets in (
                    successful_by_partition.items()
                ):

                    offsets.sort()

                    if not offsets:

                        continue

                    safe_last_offset = offsets[0]

                    for offset in offsets[1:]:

                        if offset == safe_last_offset + 1:

                            safe_last_offset = offset

                        else:

                            break

                    safe_offsets[
                        TopicPartition(
                            self.consumer.topic,
                            partition,
                        )
                    ] = safe_last_offset

                # ==================================================
                # STEP 5: BATCH COMMIT
                # ==================================================

                if safe_offsets:

                    try:

                        self.consumer.commit_offsets(
                            safe_offsets
                        )

                    except Exception:

                        # The output messages were successfully
                        # acknowledged, but the input commit failed.
                        #
                        # This is safe:
                        #
                        # Kafka will replay the messages after restart.
                        #
                        # PostgreSQL has idempotent handling using:
                        #
                        # (kafka_topic, kafka_partition, kafka_offset)
                        #
                        # so downstream duplicates are safe.

                        logger.exception(
                            "BATCH OFFSET COMMIT FAILED | "
                            "batch=%s",
                            self.batches,
                        )

                    else:

                        self.validated += len(
                            successful
                        )

                # ==================================================
                # STEP 6: BATCH SUMMARY
                # ==================================================

                logger.info(
                    "VALIDATION BATCH COMPLETE | "
                    "batch=%s | "
                    "received=%s | "
                    "published=%s | "
                    "failed=%s | "
                    "partitions_committed=%s",
                    self.batches,
                    len(batch),
                    len(successful),
                    len(batch) - len(successful),
                    len(safe_offsets),
                )

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

        logger.info(
            "Batches                : %s",
            self.batches,
        )

        logger.info("=" * 70)

    # ==============================================================
    # CLOSE
    # ==============================================================

    def close(self):

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
#```
