from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET

from src.models.traffic_event import TrafficEvent


class SNDLibXMLParser:
    """
    Parser for SNDlib XML network files.

    Converts each <demand> element into a standardized
    TrafficEvent object.
    """

    # SNDlib XML namespace
    NAMESPACE = {
        "sndlib": "http://sndlib.zib.de/network"
    }

    def __init__(self, dataset_name: str = "abilene"):

        self.dataset_name = dataset_name

    def parse_file(self, file_path: str):

        file_path = Path(file_path)

        # --------------------------------------------------
        # 1. Check file
        # --------------------------------------------------

        if not file_path.exists():

            raise FileNotFoundError(
                f"XML file not found: {file_path}"
            )

        if file_path.suffix.lower() != ".xml":

            raise ValueError(
                f"Expected XML file, got: {file_path}"
            )

        # --------------------------------------------------
        # 2. Parse XML
        # --------------------------------------------------

        try:

            tree = ET.parse(file_path)

            root = tree.getroot()

        except ET.ParseError as error:

            raise ValueError(
                f"Invalid XML file: {file_path}"
            ) from error

        # --------------------------------------------------
        # 3. Validate root
        # --------------------------------------------------

        if root.tag != "{http://sndlib.zib.de/network}network":

            raise ValueError(
                f"Unexpected XML root in {file_path}: "
                f"{root.tag}"
            )

        # --------------------------------------------------
        # 4. Extract META
        # --------------------------------------------------

        meta = root.find(
            "sndlib:meta",
            self.NAMESPACE
        )

        if meta is None:

            raise ValueError(
                f"Missing <meta> section: {file_path}"
            )

        # --------------------------------------------------
        # 5. Extract granularity
        # --------------------------------------------------

        granularity = meta.findtext(
            "sndlib:granularity",
            default="",
            namespaces=self.NAMESPACE
        ).strip()

        if not granularity:

            raise ValueError(
                f"Missing granularity: {file_path}"
            )

        # --------------------------------------------------
        # 6. Extract timestamp
        # --------------------------------------------------

        time_raw = meta.findtext(
            "sndlib:time",
            default="",
            namespaces=self.NAMESPACE
        ).strip()

        if not time_raw:

            raise ValueError(
                f"Missing timestamp: {file_path}"
            )

        try:

            timestamp = datetime.strptime(
                time_raw,
                "%Y%m%d-%H%M"
            )

        except ValueError as error:

            raise ValueError(
                f"Invalid timestamp '{time_raw}' "
                f"in {file_path}"
            ) from error

        # --------------------------------------------------
        # 7. Extract unit
        # --------------------------------------------------

        unit = meta.findtext(
            "sndlib:unit",
            default="",
            namespaces=self.NAMESPACE
        ).strip()

        if not unit:

            raise ValueError(
                f"Missing unit: {file_path}"
            )

        # --------------------------------------------------
        # 8. Extract nodes
        # --------------------------------------------------

        nodes = set()

        node_elements = root.findall(
            ".//sndlib:node",
            self.NAMESPACE
        )

        for node in node_elements:

            node_id = node.get("id")

            if node_id:

                nodes.add(node_id.strip())

        if not nodes:

            raise ValueError(
                f"No nodes found: {file_path}"
            )

        # --------------------------------------------------
        # 9. Extract demands
        # --------------------------------------------------

        demand_elements = root.findall(
            ".//sndlib:demand",
            self.NAMESPACE
        )

        if not demand_elements:

            raise ValueError(
                f"No demands found: {file_path}"
            )

        # --------------------------------------------------
        # 10. Source metadata
        # --------------------------------------------------

        source_folder = file_path.parent.name

        source_file = file_path.name

        # --------------------------------------------------
        # 11. Track duplicates
        # --------------------------------------------------

        seen_demands = set()

        # --------------------------------------------------
        # 12. Convert each demand
        # --------------------------------------------------

        for demand in demand_elements:

            # ----------------------------------------------
            # Demand ID
            # ----------------------------------------------

            demand_id = demand.get("id")

            if not demand_id:

                raise ValueError(
                    f"Demand without ID in {file_path}"
                )

            demand_id = demand_id.strip()

            # ----------------------------------------------
            # Source
            # ----------------------------------------------

            source = demand.findtext(
                "sndlib:source",
                default="",
                namespaces=self.NAMESPACE
            ).strip()

            # ----------------------------------------------
            # Target
            # ----------------------------------------------

            target = demand.findtext(
                "sndlib:target",
                default="",
                namespaces=self.NAMESPACE
            ).strip()

            # ----------------------------------------------
            # Demand value
            # ----------------------------------------------

            value_raw = demand.findtext(
                "sndlib:demandValue",
                default="",
                namespaces=self.NAMESPACE
            ).strip()

            # ----------------------------------------------
            # Validate required fields
            # ----------------------------------------------

            if not source:

                raise ValueError(
                    f"Missing source for demand "
                    f"{demand_id}"
                )

            if not target:

                raise ValueError(
                    f"Missing target for demand "
                    f"{demand_id}"
                )

            if not value_raw:

                raise ValueError(
                    f"Missing demand value for "
                    f"{demand_id}"
                )

            # ----------------------------------------------
            # Validate nodes
            # ----------------------------------------------

            if source not in nodes:

                raise ValueError(
                    f"Unknown source node "
                    f"'{source}' in {file_path}"
                )

            if target not in nodes:

                raise ValueError(
                    f"Unknown target node "
                    f"'{target}' in {file_path}"
                )

            # ----------------------------------------------
            # Source != target
            # ----------------------------------------------

            if source == target:

                raise ValueError(
                    f"Source and target are identical "
                    f"for demand {demand_id}"
                )

            # ----------------------------------------------
            # Convert traffic
            # ----------------------------------------------

            try:

                traffic_mbps = float(value_raw)

            except ValueError as error:

                raise ValueError(
                    f"Invalid demand value "
                    f"'{value_raw}' for {demand_id}"
                ) from error

            # ----------------------------------------------
            # Validate traffic
            # ----------------------------------------------

            if traffic_mbps < 0:

                raise ValueError(
                    f"Negative traffic value "
                    f"{traffic_mbps} for {demand_id}"
                )

            # ----------------------------------------------
            # Detect duplicate OD pair
            # ----------------------------------------------

            demand_key = (
                timestamp,
                source,
                target
            )

            if demand_key in seen_demands:

                raise ValueError(
                    f"Duplicate demand: "
                    f"{source} -> {target} "
                    f"at {timestamp}"
                )

            seen_demands.add(demand_key)

            # ----------------------------------------------
            # Create event ID
            # ----------------------------------------------

            event_id = (
                f"{self.dataset_name}_"
                f"{timestamp.strftime('%Y%m%d-%H%M')}_"
                f"{source}_"
                f"{target}"
            )

            # ----------------------------------------------
            # Create canonical event
            # ----------------------------------------------

            yield TrafficEvent(

                event_id=event_id,

                timestamp=timestamp,

                source_node=source,

                destination_node=target,

                traffic_mbps=traffic_mbps,

                demand_id=demand_id,

                granularity=granularity,

                unit=unit,

                dataset=self.dataset_name,

                source_format="sndlib_xml",

                source_folder=source_folder,

                source_file=source_file,

                schema_version="1.0"
            )