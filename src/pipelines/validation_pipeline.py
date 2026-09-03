from dataclasses import dataclass, field
from typing import Iterator

from src.models.traffic_event import TrafficEvent
from src.validators.traffic_validator import TrafficValidator


@dataclass
class ValidationResult:
    """
    Stores the result of validating a batch/stream
    of TrafficEvent objects.
    """

    total_events: int = 0
    valid_events: int = 0
    invalid_events: int = 0

    validation_errors: list[dict] = field(
        default_factory=list
    )


class ValidationPipeline:
    """
    Validation pipeline responsible for applying
    TrafficValidator to incoming TrafficEvent objects.

    Responsibilities:
        - Consume TrafficEvent objects
        - Validate each event
        - Separate valid and invalid events
        - Track validation statistics
        - Track validation errors

    Does NOT:
        - Parse XML
        - Discover dataset files
        - Publish to Kafka
        - Perform feature engineering
        - Perform ML prediction
    """

    def __init__(
        self,
        validator: TrafficValidator | None = None
    ):
        self.validator = (
            validator
            if validator is not None
            else TrafficValidator()
        )

    def process(
        self,
        events: Iterator[TrafficEvent]
    ) -> tuple[
        list[TrafficEvent],
        list[dict]
    ]:
        """
        Validate a stream of TrafficEvent objects.

        Parameters
        ----------
        events:
            Iterator of TrafficEvent objects.

        Returns
        -------
        tuple
            (
                valid_events,
                invalid_events
            )
        """

        valid_events = []
        invalid_events = []

        for event in events:

            errors = self.validator.validate(event)

            if errors:

                invalid_events.append(
                    {
                        "event": event,
                        "errors": errors
                    }
                )

            else:

                valid_events.append(event)

        return valid_events, invalid_events

    def process_stream(
        self,
        events: Iterator[TrafficEvent]
    ) -> Iterator[TrafficEvent]:
        """
        Validate events and yield only valid events.

        This method is useful later for Kafka
        and other streaming components.
        """

        for event in events:

            errors = self.validator.validate(event)

            if not errors:
                yield event

    def validate_event(
        self,
        event: TrafficEvent
    ) -> list[str]:
        """
        Validate a single TrafficEvent.
        """

        return self.validator.validate(event)

    