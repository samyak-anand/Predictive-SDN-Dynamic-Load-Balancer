from pathlib import Path

from src.parsers.sndlib_parser import SNDLibNativeParser


def main():

    parser = SNDLibNativeParser(
        dataset_name="abilene"
    )

    data_dir = Path(
        "/home/samyak/PycharmProjects/"
        "Predictive-SDN-Dynamic-Load-Balancer_data/"
        "directed-abilene-zhang-5min-over-6months-ALL-native"
    )

    txt_file = (
        data_dir
        / "demandMatrix-abilene-zhang-5min-20040301-0000.txt"
    )

    events = list(
        parser.parse_file(txt_file)
    )

    print(f"Total events: {len(events)}")

    for event in events[:5]:
        print(event)


if __name__ == "__main__":
    main()