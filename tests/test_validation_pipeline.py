from datetime import datetime

from src.models.traffic_event import TrafficEvent
from src.pipelines.validation_pipeline import ValidationPipeline


def create_event(
    timestamp="2004-03-01 00:00",
    source="ATLAM5",
    destination="ATLAng",
    traffic=0.522208,
):
    return TrafficEvent(
        event_id=(
            f"abilene_{timestamp}_"
            f"{source}_{destination}"
        ),
        timestamp=datetime.strptime(
            timestamp,
            "%Y-%m-%d %H:%M"
        ),
        source_node=source,
        destination_node=destination,
        traffic_mbps=traffic,
        demand_id=f"{source}_{destination}",
        granularity="5min",
        unit="MBITPERSEC",
        dataset="abilene",
        source_format="sndlib_xml",
        source_folder="test",
        source_file="test.xml",
    )


def test_valid_unique_event():

    pipeline = ValidationPipeline()

    event = create_event()

    valid, invalid, duplicates = pipeline.process(
        iter([event])
    )

    assert len(valid) == 1
    assert len(invalid) == 0
    assert len(duplicates) == 0


def test_duplicate_event_is_detected():

    pipeline = ValidationPipeline()

    event1 = create_event()
    event2 = create_event(
        traffic=0.999999
    )

    valid, invalid, duplicates = pipeline.process(
        iter([event1, event2])
    )

    assert len(valid) == 1
    assert len(invalid) == 0
    assert len(duplicates) == 1

    assert duplicates[0] == event2


def test_invalid_event_is_rejected():

    pipeline = ValidationPipeline()

    event = create_event(
        traffic=-10
    )

    valid, invalid, duplicates = pipeline.process(
        iter([event])
    )

    assert len(valid) == 0
    assert len(invalid) == 1
    assert len(duplicates) == 0


def test_different_timestamp_is_unique():

    pipeline = ValidationPipeline()

    event1 = create_event(
        timestamp="2004-03-01 00:00"
    )

    event2 = create_event(
        timestamp="2004-03-01 00:05"
    )

    valid, invalid, duplicates = pipeline.process(
        iter([event1, event2])
    )

    assert len(valid) == 2
    assert len(invalid) == 0
    assert len(duplicates) == 0


def test_different_source_is_unique():

    pipeline = ValidationPipeline()

    event1 = create_event(
        source="ATLAM5"
    )

    event2 = create_event(
        source="CHINng"
    )

    valid, invalid, duplicates = pipeline.process(
        iter([event1, event2])
    )

    assert len(valid) == 2
    assert len(invalid) == 0
    assert len(duplicates) == 0


def test_different_destination_is_unique():

    pipeline = ValidationPipeline()

    event1 = create_event(
        destination="ATLAng"
    )

    event2 = create_event(
        destination="CHINng"
    )

    valid, invalid, duplicates = pipeline.process(
        iter([event1, event2])
    )

    assert len(valid) == 2
    assert len(invalid) == 0
    assert len(duplicates) == 0


def test_process_stream_filters_duplicates():

    pipeline = ValidationPipeline()

    event1 = create_event()

    event2 = create_event(
        traffic=0.999999
    )

    events = pipeline.process_stream(
        iter([event1, event2])
    )

    result = list(events)

    assert len(result) == 1
    assert result[0] == event1


def test_process_stream_filters_invalid_events():

    pipeline = ValidationPipeline()

    valid_event = create_event()

    invalid_event = create_event(
        traffic=-5
    )

    events = pipeline.process_stream(
        iter([
            valid_event,
            invalid_event,
        ])
    )

    result = list(events)

    assert len(result) == 1
    assert result[0] == valid_event


def test_duplicate_detection_happens_after_validation():

    pipeline = ValidationPipeline()

    invalid_event = create_event(
        traffic=-10
    )

    valid_event = create_event()

    valid, invalid, duplicates = pipeline.process(
        iter([
            invalid_event,
            valid_event,
        ])
    )

    assert len(valid) == 1
    assert len(invalid) == 1
    assert len(duplicates) == 0


if __name__ == "__main__":

    print(
        "Running ValidationPipeline tests..."
    )

    test_valid_unique_event()
    test_duplicate_event_is_detected()
    test_invalid_event_is_rejected()
    test_different_timestamp_is_unique()
    test_different_source_is_unique()
    test_different_destination_is_unique()
    test_process_stream_filters_duplicates()
    test_process_stream_filters_invalid_events()
    test_duplicate_detection_happens_after_validation()

    print(
        "All ValidationPipeline tests passed."
    )