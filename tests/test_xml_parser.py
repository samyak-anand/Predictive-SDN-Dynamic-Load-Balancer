from pathlib import Path

from src.parsers.xml_parser import SNDLibXMLParser


def main():
    parser = SNDLibXMLParser(dataset_name="abilene")

    data_dir = Path(
        "/home/samyak/PycharmProjects/"
        "Predictive-SDN-Dynamic-Load-Balancer_data/"
        "directed-abilene-zhang-5min-over-6months-ALL"
    )

    xml_file = (
        data_dir
        / "demandMatrix-abilene-zhang-5min-20040301-0000.xml"
    )

    events = list(parser.parse_file(xml_file))

    print(f"Total events: {len(events)}")

    for event in events[:5]:
        print(event)


if __name__ == "__main__":
    main()