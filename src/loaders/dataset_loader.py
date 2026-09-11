from pathlib import Path
from typing import Iterator

from src.models.traffic_event import TrafficEvent
from src.parsers.xml_parser import SNDLibXMLParser
from src.parsers.sndlib_parser import SNDLibNativeParser


class DatasetLoader:
    """
    Dataset loader for the Abilene/SNDlib traffic dataset.

    Responsibilities:
        - Validate the dataset directory
        - Discover supported dataset files
        - Route files to the appropriate parser
        - Stream TrafficEvent objects
        - Support deterministic resume from a specific file

    The loader does NOT:
        - Validate traffic data
        - Publish to Kafka
        - Perform feature engineering
        - Perform ML prediction
    """

    SUPPORTED_EXTENSIONS = {".xml", ".txt"}

    def __init__(self, dataset_name: str = "abilene"):
        self.dataset_name = dataset_name

        self.xml_parser = SNDLibXMLParser(
            dataset_name=dataset_name
        )

        self.native_parser = SNDLibNativeParser(
            dataset_name=dataset_name
        )

    def load(
        self,
        directory: str | Path,
        start_file: str | None = None
    ) -> Iterator[TrafficEvent]:
        """
        Discover and stream events from the dataset directory.

        Parameters
        ----------
        directory:
            Path to the external dataset directory.

        start_file:
            Optional filename from which ingestion should begin.
            All supported files before this filename are skipped.

            Example:
                demandMatrix-abilene-zhang-5min-20040821-0055.txt

        Yields
        ------
        TrafficEvent
            Canonical traffic events.
        """

        directory = Path(directory)

        self._validate_directory(directory)

        files = self._discover_files(directory)

        if not files:
            raise FileNotFoundError(
                f"No supported dataset files found in: {directory}"
            )

        # ---------------------------------------------------------
        # Validate resume point
        # ---------------------------------------------------------

        if start_file:

            matching_files = [
                file_path
                for file_path in files
                if file_path.name == start_file
            ]

            if not matching_files:
                raise FileNotFoundError(
                    f"Start file not found in dataset: {start_file}"
                )

            if len(matching_files) > 1:
                raise ValueError(
                    f"Multiple files found with the same name: "
                    f"{start_file}"
                )

        # ---------------------------------------------------------
        # Stream files
        # ---------------------------------------------------------

        for file_path in files:

            if start_file and file_path.name < start_file:
                continue

            yield from self._load_file(file_path)

    def _discover_files(
        self,
        directory: Path
    ) -> list[Path]:
        """
        Recursively discover supported dataset files.

        XML:
            .xml → SNDlib XML parser

        Native:
            .txt → SNDlib native parser

        Files are returned in deterministic sorted order.
        """

        files = []

        for file_path in directory.rglob("*"):

            if not file_path.is_file():
                continue

            extension = file_path.suffix.lower()

            if extension in self.SUPPORTED_EXTENSIONS:
                files.append(file_path)

        # Deterministic processing order
        return sorted(files)

    def _load_file(
        self,
        file_path: Path
    ) -> Iterator[TrafficEvent]:
        """
        Route a dataset file to the appropriate parser.
        """

        extension = file_path.suffix.lower()

        if extension == ".xml":

            yield from self.xml_parser.parse_file(
                file_path
            )

        elif extension == ".txt":

            yield from self.native_parser.parse_file(
                file_path
            )

        else:

            raise ValueError(
                f"Unsupported file format: {file_path}"
            )

    @staticmethod
    def _validate_directory(
        directory: Path
    ) -> None:
        """
        Validate that the dataset path exists
        and is a directory.
        """

        if not directory.exists():
            raise FileNotFoundError(
                f"Dataset directory does not exist: {directory}"
            )

        if not directory.is_dir():
            raise ValueError(
                f"Dataset path is not a directory: {directory}"
            )