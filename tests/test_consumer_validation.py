from datetime import datetime

from src.kafka.consumer import TrafficKafkaConsumer
from src.models.traffic_event import TrafficEvent


def create_valid_event():
    return TrafficEvent(
        event_id="validation_test_001",
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


def test_valid_event():

    consumer = TrafficKafkaConsumer()

    event = create_valid_event()

    errors = consumer.validate_event(event)

    assert errors == []

    consumer.close()


def test_negative_traffic():

    consumer = TrafficKafkaConsumer()

    event = create_valid_event()

    invalid_event = TrafficEvent(
        event_id=event.event_id,
        timestamp=event.timestamp,
        source_node=event.source_node,
        destination_node=event.destination_node,
        traffic_mbps=-10.0,
        demand_id=event.demand_id,
        granularity=event.granularity,
        unit=event.unit,
        dataset=event.dataset,
        source_format=event.source_format,
        source_folder=event.source_folder,
        source_file=event.source_file,
        schema_version=event.schema_version,
    )

    errors = consumer.validate_event(
        invalid_event
    )

    assert "traffic_mbps cannot be negative" in errors

    consumer.close()


def test_same_source_destination():

    consumer = TrafficKafkaConsumer()

    event = create_valid_event()

    invalid_event = TrafficEvent(
        event_id=event.event_id,
        timestamp=event.timestamp,
        source_node="ATLAM5",
        destination_node="ATLAM5",
        traffic_mbps=10.0,
        demand_id=event.demand_id,
        granularity=event.granularity,
        unit=event.unit,
        dataset=event.dataset,
        source_format=event.source_format,
        source_folder=event.source_folder,
        source_file=event.source_file,
        schema_version=event.schema_version,
    )

    errors = consumer.validate_event(
        invalid_event
    )

    assert (
        "source and destination cannot be the same"
        in errors
    )

    consumer.close()


def main():

    test_valid_event()
    test_negative_traffic()
    test_same_source_destination()

    print(
        "All consumer validation tests passed."
    )


if __name__ == "__main__":
    main()