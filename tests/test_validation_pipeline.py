from pathlib import Path
from collections import Counter

from src.loaders.dataset_loader import DatasetLoader
from src.pipelines.validation_pipeline import ValidationPipeline


DATASET_PATH = Path(
    "/home/samyak/PycharmProjects/"
    "Predictive-SDN-Dynamic-Load-Balancer_data/"
    "directed-abilene-zhang-5min-over-6months-ALL"
)


def main():

    print("Starting XML + Native validation test...")
    print()

    loader = DatasetLoader(
        dataset_name="abilene"
    )

    pipeline = ValidationPipeline()

    events = loader.load(DATASET_PATH)

    valid_events, invalid_events = (
        pipeline.process(events)
    )

    # -----------------------------------
    # Basic validation statistics
    # -----------------------------------

    total_events = (
        len(valid_events)
        + len(invalid_events)
    )

    print("Validation Results")
    print("------------------")

    print(
        f"Total events:   {total_events}"
    )

    print(
        f"Valid events:   {len(valid_events)}"
    )

    print(
        f"Invalid events: {len(invalid_events)}"
    )

    # -----------------------------------
    # Source format analysis
    # -----------------------------------

    format_counts = Counter(
        event.source_format
        for event in valid_events
    )

    print()
    print("Source Format Distribution")
    print("--------------------------")

    for source_format, count in format_counts.items():

        print(
            f"{source_format}: {count}"
        )

    # -----------------------------------
    # Source file analysis
    # -----------------------------------

    xml_events = [
        event
        for event in valid_events
        if event.source_format == "sndlib_xml"
    ]

    native_events = [
        event
        for event in valid_events
        if event.source_format == "sndlib_native"
    ]

    print()
    print("Format Verification")
    print("-------------------")

    print(
        f"XML events:    {len(xml_events)}"
    )

    print(
        f"Native events: {len(native_events)}"
    )

    # -----------------------------------
    # File verification
    # -----------------------------------

    xml_files = set(
        event.source_file
        for event in xml_events
    )

    native_files = set(
        event.source_file
        for event in native_events
    )

    print()
    print("Files Processed")
    print("---------------")

    print(
        f"XML files:    {len(xml_files)}"
    )

    print(
        f"Native files: {len(native_files)}"
    )

    # -----------------------------------
    # Sample events
    # -----------------------------------

    print()
    print("Sample XML Event")
    print("----------------")

    if xml_events:
        print(xml_events[0])

    else:
        print("No XML events found.")

    print()
    print("Sample Native Event")
    print("-------------------")

    if native_events:
        print(native_events[0])

    else:
        print("No native events found.")

    # -----------------------------------
    # Assertions
    # -----------------------------------

    assert total_events > 0, (
        "No events were processed."
    )

    assert len(invalid_events) == 0, (
        "Invalid events were detected."
    )

    assert len(xml_events) > 0, (
        "No XML events were processed."
    )

    assert len(native_events) > 0, (
        "No native events were processed."
    )

    assert (
        len(xml_events) + len(native_events)
        == total_events
    ), (
        "XML + Native event counts do not "
        "match total events."
    )

    print()
    print(
        "SUCCESS: XML and Native data "
        "passed through the same "
        "validation pipeline."
    )


if __name__ == "__main__":
    main()