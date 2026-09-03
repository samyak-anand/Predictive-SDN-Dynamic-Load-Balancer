from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TrafficEvent:
    """
    Canonical traffic event used throughout the SDN
    predictive load-balancing data pipeline.
    """

    event_id: str
    timestamp: datetime

    source_node: str
    destination_node: str

    traffic_mbps: float

    demand_id: str

    granularity: str
    unit: str

    dataset: str

    source_format: str
    source_folder: str
    source_file: str

    schema_version: str = "1.0"