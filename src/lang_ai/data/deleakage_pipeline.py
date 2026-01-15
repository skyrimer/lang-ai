"""
Pipeline for removing political leakage patterns from preprocessed data.
"""

from pathlib import Path

import pandas as pd

from src.lang_ai.core.base_pipeline import BasePipeline
from src.lang_ai.core.logger import setup_logging
from src.lang_ai.core.paths import ProjectPaths
from src.lang_ai.data.leakage_filter import LeakageFilter

logger = setup_logging(__name__)


class DeleakagePipeline(BasePipeline):
    """
    Pipeline for removing political leakage patterns from preprocessed data.

    Inherits from BasePipeline and applies leakage filtering.
    """

    input_path: Path
    output_path: Path

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply leakage filtering to DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Deleaked DataFrame
        """
        logger.info("Applying leakage filters...")

        # Apply leakage filters
        leakage_filter = LeakageFilter()
        df_deleaked, stats = leakage_filter.apply_all(df)

        # Log statistics
        logger.info("DELEAKAGE STATISTICS")
        logger.info("\nReplacements by category:")
        for category, count in stats.items():
            logger.info(f"  {category}: {count} replacements")

        return df_deleaked


def deleakage_pipeline(
    input_path: Path = ProjectPaths.preprocessed_csv(),
    output_path: Path = ProjectPaths.deleaked_csv(),
) -> None:
    """
    Main entry point for the deleakage pipeline.

    Args:
        input_path: Path to the preprocessed data file (defaults to ProjectPaths.preprocessed_csv())
        output_path: Path where the deleaked data will be saved (defaults to ProjectPaths.deleaked_csv())
    """
    pipeline = DeleakagePipeline(input_path=input_path, output_path=output_path)
    pipeline.run()


if __name__ == "__main__":
    deleakage_pipeline()
