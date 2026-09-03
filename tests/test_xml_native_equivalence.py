from pathlib import Path

from src.parsers.xml_parser import SNDLibXMLParser
from src.parsers.sndlib_parser import SNDLibNativeParser


BASE_DIR = Path(
    "/home/samyak/PycharmProjects/"
    "Predictive-SDN-Dynamic-Load-Balancer_data"
)

XML_DIR = (
    BASE_DIR /
    "directed-abilene-zhang-5min-over-6months-ALL"
)

NATIVE_DIR = (
    BASE_DIR /
    "directed-abilene-zhang-5min-over-6months-ALL-native"
)


def main():

    xml_file = (
        XML_DIR /
        "demandMatrix-abilene-zhang-5min-20040301-0000.xml"
    )

    native_file = (
        NATIVE_DIR /
        "demandMatrix-abilene-zhang-5min-20040301-0000.txt"
    )

    xml_parser = SNDLibXMLParser(
        dataset_name="abilene"
    )

    native_parser = SNDLibNativeParser(
        dataset_name="abilene"
    )

    xml_events = list(
        xml_parser.parse_file(xml_file)
    )

    native_events = list(
        native_parser.parse_file(native_file)
    )

    print("\nXML events:", len(xml_events))
    print("Native events:", len(native_events))

    assert len(xml_events) == len(native_events)

    xml_map = {
        (
            event.timestamp,
            event.source_node,
            event.destination_node
        ): event.traffic_mbps
        for event in xml_events
    }

    native_map = {
        (
            event.timestamp,
            event.source_node,
            event.destination_node
        ): event.traffic_mbps
        for event in native_events
    }

    print("XML unique keys:", len(xml_map))
    print("Native unique keys:", len(native_map))

    assert set(xml_map.keys()) == set(native_map.keys())

    mismatches = []

    for key in xml_map:

        xml_value = xml_map[key]
        native_value = native_map[key]

        if abs(xml_value - native_value) > 1e-9:
            mismatches.append(
                (key, xml_value, native_value)
            )

    print("Value mismatches:", len(mismatches))

    assert not mismatches

    print("\nXML and Native datasets are equivalent.")
    print("All traffic observations match.")


if __name__ == "__main__":
    main()