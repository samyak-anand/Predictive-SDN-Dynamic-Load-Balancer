from dataclasses import dataclass, field
from typing import Iterator

from src.models.traffic_event import TrafficEvent
from src.validators.traffic_validator import TrafficValidator
from src.validators.duplicate_detector import DuplicateDetector


@dataclass
class ValidationResult:
    """
    Stores validation and duplicate detection statistics.
    """

    total_events: int = 0
    valid_events: int = 0
    invalid_events: int = 0
    duplicate_events: int = 0

    validation_errors: list[dict] = field(
        default_factory=list
    )

    duplicates: list[TrafficEvent] = field(
        default_factory=list
    )


class ValidationPipeline:
    """
    Validation pipeline responsible for applying:

        1. Field-level validation
        2. Duplicate detection

    Processing flow:

        TrafficEvent
             |
             v
        TrafficValidator
             |
        +----+----+
        |         |
      invalid    valid
        |         |
        v         v
      reject   DuplicateDetector
                  |
              +---+---+
              |       |
           duplicate  unique
              |       |
              v       v
            reject   valid event

    Does NOT:
        - Parse XML
        - Discover dataset files
        - Publish to Kafka
        - Perform feature engineering
        - Perform ML prediction
    """

    def __init__(
        self,
        validator: TrafficValidator | None = None,
        duplicate_detector: DuplicateDetector | None = None,
    ):
        self.validator = (
            validator
            if validator is not None
            else TrafficValidator()
        )

        self.duplicate_detector = (
            duplicate_detector
            if duplicate_detector is not None
            else DuplicateDetector()
        )

    def process(
        self,
        events: Iterator[TrafficEvent]
    ) -> tuple[
        list[TrafficEvent],
        list[dict],
        list[TrafficEvent],
    ]:
        """
        Process events through validation and duplicate detection.

        Returns:
            valid_events
            invalid_events
            duplicate_events
        """

        valid_events = []
        invalid_events = []
        duplicate_events = []

        for event in events:

            errors = self.validator.validate(event)

            if errors:

                invalid_events.append(
                    {
                        "event": event,
                        "errors": errors,
                    }
                )

                continue

            if self.duplicate_detector.is_duplicate(event):

                duplicate_events.append(event)

                continue

            valid_events.append(event)

        return (
            valid_events,
            invalid_events,
            duplicate_events,
        )

    def process_stream(
        self,
        events: Iterator[TrafficEvent]
    ) -> Iterator[TrafficEvent]:
        """
        Streaming version of the validation pipeline.

        Only valid and unique events are yielded.

        This method does not retain the valid events in memory.
        """

        for event in events:

            errors = self.validator.validate(event)

            if errors:
                continue

            if self.duplicate_detector.is_duplicate(event):
                continue

            yield event

    def validate_event(
        self,
        event: TrafficEvent
    ) -> list[str]:

        return self.validator.validate(event)