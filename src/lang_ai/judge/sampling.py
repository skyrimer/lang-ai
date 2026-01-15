"""
Sampling utilities for creating stratified or random samples from datasets.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel, Field

from src.lang_ai.core.logger import setup_logging

logger = setup_logging(__name__)


class SamplingConfig(BaseModel):
    """Configuration for dataset sampling."""

    sample_size: int = Field(gt=0, description="Number of samples to draw")
    random_seed: int = Field(default=42, description="Random seed for reproducibility")
    stratify_column: Optional[str] = Field(
        default=None,
        description="Column to stratify by (e.g., 'political_label'). If None, random sampling",
    )


def create_sample(
    input_path: Path,
    output_path: Path,
    sample_size: int,
    random_seed: int = 42,
    stratify_column: Optional[str] = None,
) -> pd.DataFrame:
    """
    Creates a sample from the input dataset.

    Args:
        input_path: Path to input CSV file
        output_path: Path to save the sampled CSV
        sample_size: Number of samples to draw
        random_seed: Random seed for reproducibility
        stratify_column: Column name to stratify by (if None, random sampling)

    Returns:
        Sampled DataFrame

    Raises:
        FileNotFoundError: If input file doesn't exist
        ValueError: If sample_size exceeds dataset size or stratification fails
    """
    logger.info(f"Loading data from {input_path}")

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)
    logger.info(f"Loaded {len(df)} rows")

    if sample_size > len(df):
        raise ValueError(
            f"Sample size ({sample_size}) exceeds dataset size ({len(df)})"
        )

    # Perform sampling
    if stratify_column:
        if stratify_column not in df.columns:
            raise ValueError(
                f"Stratification column '{stratify_column}' not found in dataset"
            )

        logger.info(f"Performing stratified sampling by '{stratify_column}'")
        # Stratified sampling
        try:
            df_sampled = df.groupby(stratify_column, group_keys=False).apply(
                lambda x: x.sample(
                    n=int(sample_size * len(x) / len(df)),
                    random_state=random_seed,
                ),
                include_groups=False
            )

            # Handle rounding issues: add/remove samples to reach exact sample_size
            current_size = len(df_sampled)
            if current_size < sample_size:
                # Need more samples
                remaining = df[~df.index.isin(df_sampled.index)]
                additional = remaining.sample(
                    n=sample_size - current_size, random_state=random_seed
                )
                df_sampled = pd.concat([df_sampled, additional])
            elif current_size > sample_size:
                # Need fewer samples
                df_sampled = df_sampled.sample(n=sample_size, random_state=random_seed)

        except ValueError as e:
            logger.error(
                f"Stratified sampling failed: {e}. Falling back to random sampling."
            )
            df_sampled = df.sample(n=sample_size, random_state=random_seed)

    else:
        logger.info("Performing random sampling")
        df_sampled = df.sample(n=sample_size, random_state=random_seed)

    # Save sample
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_sampled.to_csv(output_path, index=False)
    logger.info(f"Saved {len(df_sampled)} samples to {output_path}")

    return df_sampled