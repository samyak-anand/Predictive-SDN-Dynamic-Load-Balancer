from datetime import datetime

from src.models.traffic_event import TrafficEvent


class DuplicateDetector:
    """
    Stateful duplicate detector for TrafficEvent objects.

    An event is considered a duplicate when another event with the
    same timestamp, source node, and destination node has already
    been processed.

    Duplicate key:
        (timestamp, source_node, destination_node)

    The detector is intentionally separate from TrafficValidator
    because duplicate detection requires state across events.
    """

    def __init__(self):
        self._seen_keys: set[
            tuple[datetime, str, str]
        ] = set()

    @staticmethod
    def _build_key(
        event: TrafficEvent
    ) -> tuple[datetime, str, str]:
        return (
            event.timestamp,
            event.source_node,
            event.destination_node,
        )

    def is_duplicate(
        self,
        event: TrafficEvent
    ) -> bool:
        """
        Check whether the event has already been seen.

        If the event is new, its key is registered and False is returned.

        If the event was already seen, True is returned.
        """

        key = self._build_key(event)

        if key in self._seen_keys:
            return True

        self._seen_keys.add(key)

        return False

    def add(
        self,
        event: TrafficEvent
    ) -> None:
        """
        Explicitly register an event as seen.
        """

        key = self._build_key(event)

        self._seen_keys.add(key)

    def has_seen(
        self,
        event: TrafficEvent
    ) -> bool:
        """
        Check whether an event has already been registered.

        Unlike is_duplicate(), this method does not modify state.
        """

        key = self._build_key(event)

        return key in self._seen_keys

    @property
    def duplicate_key_count(self) -> int:
        """
        Number of unique event keys currently stored.
        """

        return len(self._seen_keys)

    def reset(self) -> None:
        """
        Clear all previously seen event keys.
        """

        self._seen_keys.clear()
