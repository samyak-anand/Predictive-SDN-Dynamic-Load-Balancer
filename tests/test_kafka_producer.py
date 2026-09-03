from datetime import datetime

from src.kafka.producer import TrafficKafkaProducer
from src.models.traffic_event import TrafficEvent


def main():

    event = TrafficEvent(
        event_id="python_test_001",
        timestamp=datetime(
            2004,
            3,
            1,
            0,
            0,
        ),
        source_node="ATLAM5",
        destination_node="ATLAng",
        traffic_mbps=0.522208,
        demand_id="ATLAM5_ATLAng",
        granularity="5min",
        unit="MBITPERSEC",
        dataset="abilene",
        source_format="sndlib_native",
        source_folder=(
            "directed-abilene-zhang-5min-over-6months-ALL-native"
        ),
        source_file=(
            "demandMatrix-abilene-zhang-5min-20040301-0000.txt"
        ),
    )

    producer = TrafficKafkaProducer(
        bootstrap_servers="localhost:9092",
        topic="traffic.raw",
    )

    producer.send(event)

    producer.flush()
    producer.close()

    print("Kafka Python producer test completed.")


if __name__ == "__main__":
    main()