from pathlib import Path

from src.loaders.dataset_loader import DatasetLoader
from src.pipelines.validation_pipeline import ValidationPipeline


PROJECT_ROOT = Path(__file__).resolve().parents[1]

XML_DATASET = (
    PROJECT_ROOT.parent
    / "Predictive-SDN-Dynamic-Load-Balancer_data"
    / "directed-abilene-zhang-5min-over-6months-ALL"
)

NATIVE_DATASET = (
    PROJECT_ROOT.parent
    / "Predictive-SDN-Dynamic-Load-Balancer_data"
    / "directed-abilene-zhang-5min-over-6months-ALL-native"
)


def run_validation(dataset_path: Path, dataset_name: str):
    print()
    print("=" * 60)
    print(f"Testing: {dataset_name}")
    print(f"Dataset: {dataset_path}")
    print("=" * 60)

    loader = DatasetLoader(dataset_name="abilene")
    pipeline = ValidationPipeline()

    events = loader.load(dataset_path)

    total = 0
    valid = 0
    invalid = 0
    duplicates = 0

    # Use the streaming pipeline.
    for event in events:
        total += 1

        # Validation
        errors = pipeline.validate_event(event)

        if errors:
            invalid += 1
            continue

        # Duplicate detection
        if pipeline.duplicate_detector.is_duplicate(event):
            duplicates += 1
            continue

        valid += 1

    print()
    print("Validation Results")
    print("------------------")
    print(f"Total events:     {total}")
    print(f"Valid events:     {valid}")
    print(f"Invalid events:   {invalid}")
    print(f"Duplicates:       {duplicates}")
    print(
        f"Unique keys:      "
        f"{pipeline.duplicate_detector.duplicate_key_count}"
    )

    assert total > 0
    assert valid > 0

    print()
    print(f"{dataset_name} validation completed successfully.")


def test_xml_real_data():
    run_validation(
        XML_DATASET,
        "SNDlib XML"
    )


def test_native_real_data():
    run_validation(
        NATIVE_DATASET,
        "SNDlib Native TXT"
    )


if __name__ == "__main__":

    print("Running real-data validation tests...")

    test_xml_real_data()
    test_native_real_data()

    print()
    print("All real-data validation tests passed.")