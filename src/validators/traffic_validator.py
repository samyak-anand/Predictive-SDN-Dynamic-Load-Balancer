import math

from src.models.traffic_event import TrafficEvent


class TrafficValidator:
    """
    Validates canonical TrafficEvent objects before
    they enter the streaming pipeline.

    Responsibilities:
        - Schema validation
        - Required-field validation
        - Traffic value validation
        - Unit validation
        - Granularity validation
        - Schema-version validation

    The validator does NOT:
        - Parse XML/native files
        - Publish to Kafka
        - Perform feature engineering
        - Perform ML prediction
    """

    SUPPORTED_UNITS = {
        "MBITPERSEC"
    }

    SUPPORTED_GRANULARITY = {
        "5min"
    }

    SUPPORTED_SCHEMA_VERSIONS = {
        "1.0"
    }

    def validate(
        self,
        event: TrafficEvent
    ) -> list[str]:
        """
        Validate a TrafficEvent.

        Returns
        -------
        list[str]
            Empty list means the event is valid.
            Otherwise, contains validation errors.
        """

        errors = []

        # -------------------------
        # Required fields
        # -------------------------

        if not event.event_id:
            errors.append(
                "event_id is missing"
            )

        if event.timestamp is None:
            errors.append(
                "timestamp is missing"
            )

        if not event.source_node:
            errors.append(
                "source_node is missing"
            )

        if not event.destination_node:
            errors.append(
                "destination_node is missing"
            )

        if not event.demand_id:
            errors.append(
                "demand_id is missing"
            )

        # -------------------------
        # Source / destination
        # -------------------------

        if (
            event.source_node
            and event.destination_node
            and event.source_node
            == event.destination_node
        ):
            errors.append(
                "source and destination "
                "cannot be the same"
            )

        # -------------------------
        # Traffic validation
        # -------------------------

        if not isinstance(
            event.traffic_mbps,
            (int, float)
        ):
            errors.append(
                "traffic_mbps must be numeric"
            )

        elif not math.isfinite(
            event.traffic_mbps
        ):
            errors.append(
                "traffic_mbps must be finite"
            )

        elif event.traffic_mbps < 0:
            errors.append(
                "traffic_mbps cannot be negative"
            )

        # -------------------------
        # Unit validation
        # -------------------------

        if event.unit not in self.SUPPORTED_UNITS:
            errors.append(
                f"unsupported unit: {event.unit}"
            )

        # -------------------------
        # Granularity validation
        # -------------------------

        if (
            event.granularity
            not in self.SUPPORTED_GRANULARITY
        ):
            errors.append(
                f"unsupported granularity: "
                f"{event.granularity}"
            )

        # -------------------------
        # Schema version
        # -------------------------

        if (
            event.schema_version
            not in self.SUPPORTED_SCHEMA_VERSIONS
        ):
            errors.append(
                f"unsupported schema version: "
                f"{event.schema_version}"
            )

        return errors

    def is_valid(
        self,
        event: TrafficEvent
    ) -> bool:
        """
        Return True when the event passes
        all validation checks.
        """

        return len(
            self.validate(event)
        ) == 0