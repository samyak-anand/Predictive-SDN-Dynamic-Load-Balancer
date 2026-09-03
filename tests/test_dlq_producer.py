import json

from src.kafka.dlq_producer import TrafficDLQProducer


def main():

    producer = TrafficDLQProducer(
        bootstrap_servers="localhost:9092",
        topic="traffic.dlq",
    )

    # Simulate a bad Kafka message.
    payload = json.dumps(
        {
            "event_id": "dlq_test_001",
            "traffic_mbps": -10,
        }
    ).encode("utf-8")

    producer.send(
        original_topic="traffic.raw",
        partition=0,
        offset=999,

        payload=payload,

        error_type="TrafficValidationError",

        error_message=(
            "traffic_mbps cannot be negative"
        ),

        validation_stage="traffic_validation",

        event_id="dlq_test_001",
    )

    producer.flush()
    producer.close()

    print(
        "DLQ producer test completed."
    )


if __name__ == "__main__":
    main()