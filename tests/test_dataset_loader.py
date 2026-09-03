from pathlib import Path

from src.loaders.dataset_loader import DatasetLoader


DATASET_PATH = Path(
    "/home/samyak/PycharmProjects/"
    "Predictive-SDN-Dynamic-Load-Balancer_data/"
    "directed-abilene-zhang-5min-over-6months-ALL"
)


def main():

    loader = DatasetLoader(
        dataset_name="abilene"
    )

    event_count = 0

    for event in loader.load(DATASET_PATH):

        event_count += 1

        if event_count <= 5:
            print(event)

    print()
    print(f"Total events loaded: {event_count}")

    print(loader.statistics.report())


if __name__ == "__main__":
    main()