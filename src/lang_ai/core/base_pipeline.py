"""
Abstract base class for data processing pipelines.

Provides common infrastructure for pipelines that load data, transform it,
and save the results.
"""

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field

from src.lang_ai.core.data_io import DataLoader
from src.lang_ai.core.logger import setup_logging

logger = setup_logging(__name__)


class BasePipeline(BaseModel, ABC):
    """
    Abstract base for data processing pipelines.

    Provides standard load -> transform -> save workflow.
    Subclasses must implement the `transform()` method.

    Attributes:
        input_path: Path to input data file
        output_path: Path to save transformed data
    """

    input_path: Path = Field(..., description="Path to input data file")
    output_path: Path = Field(..., description="Path to save output data")

    def load_data(self) -> pd.DataFrame:
        """
        Load data from input_path.

        Returns:
            Loaded DataFrame

        Raises:
            FileNotFoundError: If input file doesn't exist
        """
        df = DataLoader.load_dataframe(self.input_path)
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        return df

    def save_data(self, df: pd.DataFrame) -> None:
        """
        Save DataFrame to output_path.

        Args:
            df: DataFrame to save

        Note:
            Automatically creates parent directories if needed
        """
        logger.info(f"Saving data to {self.output_path}")
        DataLoader.save_csv(df, self.output_path, create_dirs=True)
        logger.info(f"Saved {len(df)} rows to {self.output_path}")

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformation logic to DataFrame.

        This method must be implemented by subclasses.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        pass

    def run(self) -> None:
        """
        Execute the full pipeline: load -> transform -> save.

        This is the main entry point for running the pipeline.
        """
        logger.info(f"Starting pipeline: {self.__class__.__name__}")

        # Load
        df = self.load_data()
        initial_shape = df.shape

        # Transform
        df_transformed = self.transform(df)
        final_shape = df_transformed.shape

        # Log shape change
        logger.info(f"Shape change: {initial_shape} → {final_shape}")

        # Save
        self.save_data(df_transformed)

        logger.info(f"Pipeline completed: {self.__class__.__name__}")
