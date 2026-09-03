from datetime import datetime

from src.models.traffic_event import TrafficEvent
from src.validators.duplicate_detector import DuplicateDetector


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


def test_first_event_is_not_duplicate():

    detector = DuplicateDetector()

    event = create_event()

    assert detector.is_duplicate(event) is False


def test_same_event_is_duplicate():

    detector = DuplicateDetector()

    event = create_event()

    assert detector.is_duplicate(event) is False
    assert detector.is_duplicate(event) is True


def test_different_timestamp_is_not_duplicate():

    detector = DuplicateDetector()

    event1 = create_event(
        timestamp="2004-03-01 00:00"
    )

    event2 = create_event(
        timestamp="2004-03-01 00:05"
    )

    assert detector.is_duplicate(event1) is False
    assert detector.is_duplicate(event2) is False


def test_different_source_is_not_duplicate():

    detector = DuplicateDetector()

    event1 = create_event(
        source="ATLAM5"
    )

    event2 = create_event(
        source="CHINng"
    )

    assert detector.is_duplicate(event1) is False
    assert detector.is_duplicate(event2) is False


def test_different_destination_is_not_duplicate():

    detector = DuplicateDetector()

    event1 = create_event(
        destination="ATLAng"
    )

    event2 = create_event(
        destination="CHINng"
    )

    assert detector.is_duplicate(event1) is False
    assert detector.is_duplicate(event2) is False


def test_different_traffic_value_is_still_duplicate():

    detector = DuplicateDetector()

    event1 = create_event(
        traffic=0.522208
    )

    event2 = create_event(
        traffic=0.999999
    )

    assert detector.is_duplicate(event1) is False
    assert detector.is_duplicate(event2) is True


def test_has_seen_does_not_modify_state():

    detector = DuplicateDetector()

    event = create_event()

    assert detector.has_seen(event) is False
    assert detector.has_seen(event) is False

    assert detector.duplicate_key_count == 0


def test_add_registers_event():

    detector = DuplicateDetector()

    event = create_event()

    detector.add(event)

    assert detector.has_seen(event) is True
    assert detector.duplicate_key_count == 1


def test_reset_clears_state():

    detector = DuplicateDetector()

    event = create_event()

    detector.add(event)

    assert detector.has_seen(event) is True

    detector.reset()

    assert detector.has_seen(event) is False
    assert detector.duplicate_key_count == 0


def test_multiple_unique_events():

    detector = DuplicateDetector()

    events = [
        create_event(
            timestamp="2004-03-01 00:00",
            source="ATLAM5",
            destination="ATLAng",
        ),
        create_event(
            timestamp="2004-03-01 00:00",
            source="ATLAM5",
            destination="CHINng",
        ),
        create_event(
            timestamp="2004-03-01 00:05",
            source="ATLAM5",
            destination="ATLAng",
        ),
    ]

    for event in events:
        assert detector.is_duplicate(event) is False

    assert detector.duplicate_key_count == 3


if __name__ == "__main__":
    print("Running DuplicateDetector tests...")

    test_first_event_is_not_duplicate()
    test_same_event_is_duplicate()
    test_different_timestamp_is_not_duplicate()
    test_different_source_is_not_duplicate()
    test_different_destination_is_not_duplicate()
    test_different_traffic_value_is_still_duplicate()
    test_has_seen_does_not_modify_state()
    test_add_registers_event()
    test_reset_clears_state()
    test_multiple_unique_events()

    print("All DuplicateDetector tests passed.")