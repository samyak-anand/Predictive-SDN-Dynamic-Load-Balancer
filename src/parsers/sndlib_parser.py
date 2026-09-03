from pathlib import Path
from datetime import datetime
import math
import re

from src.models.traffic_event import TrafficEvent


class SNDLibNativeParser:
    """
    Parser for SNDlib native network files.

    Converts SNDlib native demand records into the
    canonical TrafficEvent model.
    """

    EXPECTED_HEADER = "?SNDlib native format"

    def __init__(self, dataset_name="abilene"):
        self.dataset_name = dataset_name

    def parse_file(self, file_path):
        """
        Parse one SNDlib native file and yield TrafficEvent objects.
        """

        file_path = Path(file_path)

        self._validate_file(file_path)

        content = file_path.read_text(
            encoding="utf-8"
        )

        self._validate_header(content)

        meta = self._parse_metadata(content)

        timestamp = self._parse_timestamp(
            meta["time"]
        )

        nodes = self._parse_nodes(content)

        demands = self._parse_demands(content)

        seen_events = set()

        for demand in demands:

            demand_id = demand["demand_id"]
            source = demand["source"]
            destination = demand["destination"]
            traffic_mbps = demand["traffic_mbps"]

            # Validate source node
            if source not in nodes:
                raise ValueError(
                    f"Unknown source node '{source}' "
                    f"in demand '{demand_id}'"
                )

            # Validate destination node
            if destination not in nodes:
                raise ValueError(
                    f"Unknown destination node '{destination}' "
                    f"in demand '{demand_id}'"
                )

            # Source and destination must differ
            if source == destination:
                raise ValueError(
                    f"Source and destination are identical "
                    f"for demand '{demand_id}'"
                )

            # Validate traffic
            if not math.isfinite(traffic_mbps):
                raise ValueError(
                    f"Invalid traffic value for "
                    f"demand '{demand_id}'"
                )

            if traffic_mbps < 0:
                raise ValueError(
                    f"Negative traffic value for "
                    f"demand '{demand_id}'"
                )

            # Detect duplicate OD events
            event_key = (
                timestamp,
                source,
                destination
            )

            if event_key in seen_events:
                raise ValueError(
                    f"Duplicate demand detected: "
                    f"{source} -> {destination}"
                )

            seen_events.add(event_key)

            event_id = (
                f"{self.dataset_name}_"
                f"{meta['time']}_"
                f"{source}_"
                f"{destination}"
            )

            yield TrafficEvent(
                event_id=event_id,
                timestamp=timestamp,
                source_node=source,
                destination_node=destination,
                traffic_mbps=traffic_mbps,
                demand_id=demand_id,
                granularity=meta["granularity"],
                unit=meta["unit"],
                dataset=self.dataset_name,
                source_format="sndlib_native",
                source_folder=file_path.parent.name,
                source_file=file_path.name,
                schema_version="1.0",
            )

    # ---------------------------------------------------------
    # File validation
    # ---------------------------------------------------------

    def _validate_file(self, file_path):
        if not file_path.exists():
            raise FileNotFoundError(
                f"File not found: {file_path}"
            )

        if not file_path.is_file():
            raise ValueError(
                f"Path is not a file: {file_path}"
            )

    def _validate_header(self, content):

        first_line = content.lstrip().splitlines()[0]

        if not first_line.startswith(
            self.EXPECTED_HEADER
        ):
            raise ValueError(
                "File does not appear to be a valid "
                "SNDlib native file"
            )

    # ---------------------------------------------------------
    # Section extraction
    # ---------------------------------------------------------

    def _extract_section(self, content, section_name):
        """
        Extract a complete SNDlib section while correctly
        handling nested parentheses.
        """

        pattern = rf"\b{section_name}\s*\("
        match = re.search(pattern, content)

        if not match:
            raise ValueError(
                f"Missing section: {section_name}"
            )

        opening_paren = content.find(
            "(",
            match.start()
        )

        depth = 0

        for index in range(
            opening_paren,
            len(content)
        ):

            char = content[index]

            if char == "(":
                depth += 1

            elif char == ")":
                depth -= 1

                if depth == 0:
                    return content[
                        opening_paren + 1:index
                    ]

        raise ValueError(
            f"Unclosed section: {section_name}"
        )

    # ---------------------------------------------------------
    # META
    # ---------------------------------------------------------

    def _parse_metadata(self, content):

        section = self._extract_section(
            content,
            "META"
        )

        metadata = {}

        for line in section.splitlines():

            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split(
                "=",
                1
            )

            metadata[key.strip()] = value.strip()

        required = [
            "granularity",
            "time",
            "unit",
        ]

        for field in required:

            if field not in metadata:
                raise ValueError(
                    f"Missing META field: {field}"
                )

        return metadata

    # ---------------------------------------------------------
    # Timestamp
    # ---------------------------------------------------------

    def _parse_timestamp(self, timestamp_string):

        try:
            return datetime.strptime(
                timestamp_string,
                "%Y%m%d-%H%M"
            )

        except ValueError as exc:

            raise ValueError(
                f"Invalid timestamp: "
                f"{timestamp_string}"
            ) from exc

    # ---------------------------------------------------------
    # NODES
    # ---------------------------------------------------------

    def _parse_nodes(self, content):

        section = self._extract_section(
            content,
            "NODES"
        )

        nodes = {}

        pattern = re.compile(
            r"^\s*([A-Za-z0-9_]+)"
            r"\s*\(\s*"
            r"([-+]?\d+(?:\.\d+)?)"
            r"\s+"
            r"([-+]?\d+(?:\.\d+)?)"
            r"\s*\)"
        )

        for line in section.splitlines():

            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            match = pattern.match(line)

            if not match:
                continue

            node_id = match.group(1)

            longitude = float(
                match.group(2)
            )

            latitude = float(
                match.group(3)
            )

            nodes[node_id] = {
                "longitude": longitude,
                "latitude": latitude,
            }

        if not nodes:
            raise ValueError(
                "No nodes found in NODES section"
            )

        return nodes

    # ---------------------------------------------------------
    # DEMANDS
    # ---------------------------------------------------------

    def _parse_demands(self, content):

        section = self._extract_section(
            content,
            "DEMANDS"
        )

        demands = []

        pattern = re.compile(
            r"^\s*"
            r"(\S+)"
            r"\s*\(\s*"
            r"(\S+)\s+"
            r"(\S+)"
            r"\s*\)"
            r"\s+"
            r"(\S+)"
            r"\s+"
            r"([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)"
            r"\s+"
            r"(\S+)"
            r"\s*$"
        )

        for line in section.splitlines():

            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            match = pattern.match(line)

            if not match:
                raise ValueError(
                    f"Invalid DEMANDS line: {line}"
                )

            demand_id = match.group(1)
            source = match.group(2)
            destination = match.group(3)
            routing_unit = match.group(4)
            traffic_mbps = float(match.group(5))
            max_path_length = match.group(6)

            demands.append({
                "demand_id": demand_id,
                "source": source,
                "destination": destination,
                "routing_unit": routing_unit,
                "traffic_mbps": traffic_mbps,
                "max_path_length": max_path_length,
            })

        if not demands:
            raise ValueError(
                "No demands found in DEMANDS section"
            )

        return demands