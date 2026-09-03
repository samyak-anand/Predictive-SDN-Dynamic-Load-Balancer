import logging

from src.kafka.consumer import TrafficKafkaConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def main():

    consumer = TrafficKafkaConsumer(
        bootstrap_servers="localhost:9092",
        topic="traffic.raw",
        group_id="traffic-consumer-test",
        auto_offset_reset="earliest",
    )

    count = 0

    for event in consumer.consume():

        print(
            "CONSUMED EVENT:",
            event,
        )

        count += 1

        if count >= 10:
            break

    consumer.close()

    print(
        f"Kafka consumer test completed | consumed={count}"
    )


if __name__ == "__main__":
    main()