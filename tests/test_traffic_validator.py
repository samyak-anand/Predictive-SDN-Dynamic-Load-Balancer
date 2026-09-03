from datetime import datetime

from src.models.traffic_event import TrafficEvent
from src.validators.traffic_validator import (
    TrafficValidator
)


def create_valid_event():
    return TrafficEvent(
        event_id="test_001",
        timestamp=datetime(
            2004,
            3,
            1,
            0,
            0
        ),
        source_node="ATLAM5",
        destination_node="ATLAng",
        traffic_mbps=0.522208,
        demand_id="ATLAM5_ATLAng",
        granularity="5min",
        unit="MBITPERSEC",
        dataset="abilene",
        source_format="sndlib_xml",
        source_folder="test",
        source_file="test.xml",
        schema_version="1.0"
    )


def test_valid_event():

    validator = TrafficValidator()

    event = create_valid_event()

    errors = validator.validate(event)

    assert errors == []

    assert validator.is_valid(event) is True


def test_negative_traffic():

    validator = TrafficValidator()

    event = create_valid_event()

    event = TrafficEvent(
        **{
            **event.__dict__,
            "traffic_mbps": -10
        }
    )

    errors = validator.validate(event)

    assert "traffic_mbps cannot be negative" in errors


def test_nan_traffic():

    validator = TrafficValidator()

    event = create_valid_event()

    event = TrafficEvent(
        **{
            **event.__dict__,
            "traffic_mbps": float("nan")
        }
    )

    errors = validator.validate(event)

    assert "traffic_mbps must be finite" in errors


def test_infinite_traffic():

    validator = TrafficValidator()

    event = create_valid_event()

    event = TrafficEvent(
        **{
            **event.__dict__,
            "traffic_mbps": float("inf")
        }
    )

    errors = validator.validate(event)

    assert "traffic_mbps must be finite" in errors


def test_same_source_destination():

    validator = TrafficValidator()

    event = create_valid_event()

    event = TrafficEvent(
        **{
            **event.__dict__,
            "destination_node": "ATLAM5"
        }
    )

    errors = validator.validate(event)

    assert (
        "source and destination cannot be the same"
        in errors
    )


def test_invalid_unit():

    validator = TrafficValidator()

    event = create_valid_event()

    event = TrafficEvent(
        **{
            **event.__dict__,
            "unit": "GBITPERSEC"
        }
    )

    errors = validator.validate(event)

    assert (
        "unsupported unit: GBITPERSEC"
        in errors
    )


def test_invalid_granularity():

    validator = TrafficValidator()

    event = create_valid_event()

    event = TrafficEvent(
        **{
            **event.__dict__,
            "granularity": "1min"
        }
    )

    errors = validator.validate(event)

    assert (
        "unsupported granularity: 1min"
        in errors
    )


def test_invalid_schema_version():

    validator = TrafficValidator()

    event = create_valid_event()

    event = TrafficEvent(
        **{
            **event.__dict__,
            "schema_version": "2.0"
        }
    )

    errors = validator.validate(event)

    assert (
        "unsupported schema version: 2.0"
        in errors
    )


if __name__ == "__main__":
    print("Running TrafficValidator tests...")

    test_valid_event()
    test_negative_traffic()
    test_nan_traffic()
    test_infinite_traffic()
    test_same_source_destination()
    test_invalid_unit()
    test_invalid_granularity()
    test_invalid_schema_version()

    print("All TrafficValidator tests passed.")