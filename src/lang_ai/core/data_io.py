"""
Unified data loading and saving utilities.

Provides standardized CSV/JSON I/O operations with error handling,
path validation, and logging.
"""

from pathlib import Path
from typing import List, Optional

import pandas as pd

from src.lang_ai.core.logger import setup_logging

logger = setup_logging(__name__)


class DataLoader:
    """
    Centralized data loading and saving operations.

    All file I/O for DataFrames should go through this class to ensure
    consistent error handling, logging, and path management.
    """

    @staticmethod
    def load_csv(
        path: Path | str, required_columns: Optional[List[str]] = None, **kwargs
    ) -> pd.DataFrame:
        """
        Load CSV file with validation and error handling.

        Args:
            path: Path to CSV file.
            required_columns: List of columns that must be present (optional).
            **kwargs: Additional arguments passed to pd.read_csv.

        Returns:
            Loaded DataFrame.

        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If required columns are missing.
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        logger.info(f"Loading CSV from {path}")

        try:
            df = pd.read_csv(path, **kwargs)

            # Validate required columns
            if required_columns:
                missing_cols = [
                    col for col in required_columns if col not in df.columns
                ]
                if missing_cols:
                    raise ValueError(
                        f"Missing required columns: {missing_cols}. "
                        f"Available columns: {list(df.columns)}"
                    )
                logger.debug(f"All required columns present: {required_columns}")

            return df

        except Exception as e:
            logger.error(f"Failed to load CSV from {path}: {e}")
            raise

    @staticmethod
    def load_json(
        path: Path | str,
        lines: bool = True,
        required_columns: Optional[List[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Load JSON file with validation and error handling.

        Args:
            path: Path to JSON file.
            lines: If True, load as JSON lines format (one JSON object per line).
            required_columns: List of columns that must be present (optional).
            **kwargs: Additional arguments passed to pd.read_json.

        Returns:
            Loaded DataFrame.

        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If required columns are missing.
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        logger.info(f"Loading JSON from {path} (lines={lines})")

        try:
            df = pd.read_json(path, lines=lines, **kwargs)
            logger.info(
                f"Successfully loaded {len(df)} rows, {len(df.columns)} columns"
            )

            # Validate required columns
            if required_columns:
                missing_cols = [
                    col for col in required_columns if col not in df.columns
                ]
                if missing_cols:
                    raise ValueError(
                        f"Missing required columns: {missing_cols}. "
                        f"Available columns: {list(df.columns)}"
                    )
                logger.debug(f"All required columns present: {required_columns}")

            return df

        except Exception as e:
            logger.error(f"Failed to load JSON from {path}: {e}")
            raise

    @staticmethod
    def load_dataframe(
        path: Path | str, required_columns: Optional[List[str]] = None, **kwargs
    ) -> pd.DataFrame:
        """
        Auto-detect file format and load DataFrame.

        Args:
            path: Path to data file (.csv or .json).
            required_columns: List of columns that must be present (optional).
            **kwargs: Additional arguments passed to pandas read functions.

        Returns:
            Loaded DataFrame.

        Raises:
            ValueError: If file format not supported.
        """
        path = Path(path)
        suffix = path.suffix.lower()

        if suffix == ".csv":
            return DataLoader.load_csv(path, required_columns, **kwargs)
        elif suffix == ".json":
            return DataLoader.load_json(path, required_columns, **kwargs)
        else:
            raise ValueError(
                f"Unsupported file format: {suffix}. Supported formats: .csv, .json"
            )

    @staticmethod
    def save_csv(
        df: pd.DataFrame, path: Path | str, create_dirs: bool = True, **kwargs
    ) -> None:
        """
        Save DataFrame to CSV with automatic directory creation.

        Args:
            df: DataFrame to save.
            path: Output path for CSV file.
            create_dirs: If True, create parent directories if they don't exist.
            **kwargs: Additional arguments passed to df.to_csv.

        Raises:
            OSError: If directory creation or file writing fails.
        """
        path = Path(path)

        if create_dirs:
            path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {path.parent}")

        logger.info(f"Saving {len(df)} rows to {path}")

        try:
            df.to_csv(path, index=False, **kwargs)
            logger.info(f"Successfully saved to {path}")

        except Exception as e:
            logger.error(f"Failed to save CSV to {path}: {e}")
            raise

    @staticmethod
    def save_json(
        df: pd.DataFrame,
        path: Path | str,
        create_dirs: bool = True,
        orient: str = "records",
        lines: bool = True,
        **kwargs,
    ) -> None:
        """
        Save DataFrame to JSON with automatic directory creation.

        Args:
            df: DataFrame to save.
            path: Output path for JSON file.
            create_dirs: If True, create parent directories if they don't exist.
            orient: Format of JSON string (default: 'records').
            lines: If True, write as JSON lines format.
            **kwargs: Additional arguments passed to df.to_json.

        Raises:
            OSError: If directory creation or file writing fails.
        """
        path = Path(path)

        if create_dirs:
            path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {path.parent}")

        logger.info(f"Saving {len(df)} rows to {path}")

        try:
            df.to_json(path, orient=orient, lines=lines, **kwargs)
            logger.info(f"Successfully saved to {path}")

        except Exception as e:
            logger.error(f"Failed to save JSON to {path}: {e}")
            raise

    @staticmethod
    def append_to_csv(
        df: pd.DataFrame, path: Path | str, create_dirs: bool = True, **kwargs
    ) -> None:
        """
        Append DataFrame to existing CSV file or create new one.

        Args:
            df: DataFrame to append.
            path: Output path for CSV file.
            create_dirs: If True, create parent directories if they don't exist.
            **kwargs: Additional arguments passed to df.to_csv.

        Raises:
            OSError: If directory creation or file writing fails.
        """
        path = Path(path)

        if create_dirs:
            path.parent.mkdir(parents=True, exist_ok=True)

        # Determine if header should be written
        header = not path.exists()

        logger.info(f"Appending {len(df)} rows to {path} (header={header})")

        try:
            df.to_csv(path, mode="a", index=False, header=header, **kwargs)
            logger.debug(f"Successfully appended to {path}")

        except Exception as e:
            logger.error(f"Failed to append to CSV at {path}: {e}")
            raise
