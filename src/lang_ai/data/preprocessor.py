"""
Data preprocessing pipeline for the lang-ai project.
"""

from collections import Counter
from pathlib import Path
from typing import Callable

import pandas as pd
import swifter  # noqa: F401
from pydantic import BaseModel, Field

from src.lang_ai.analysis.similarity import (
    SimilarityAnalyzer,
    resolve_similarity_clusters,
)
from src.lang_ai.core.base_pipeline import BasePipeline
from src.lang_ai.core.logger import setup_logging
from src.lang_ai.core.paths import ProjectPaths
from src.lang_ai.data.filters import PollutionFilter
from src.lang_ai.data.normalizer import TextNormalizer

logger = setup_logging(__name__)


def fix_typo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixes typos in the DataFrame column names.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with corrected column names.
    """
    return df.rename(columns={"auhtor_ID": "author"})


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes authors with duplicate posts to avoid stylometric bias.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with potentially duplicate posts removed.
    """
    duplicates = df.duplicated(subset="post")
    authors_with_duplicates = df[duplicates]["author"].unique()
    logger.info(f"Authors with duplicate posts: {authors_with_duplicates}")
    logger.info(
        f"Removing {df['author'].isin(authors_with_duplicates).sum()} potentially duplicate posts..."
    )
    df = df[~df["author"].isin(authors_with_duplicates)]
    logger.info(f"Total number of unique posts: {df.shape[0]}")
    return df


def clean_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the text content of the posts.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with normalized post content.
    """
    logger.info("Normalizing text content...")
    df["post"] = df["post"].swifter.apply(TextNormalizer().normalize_text)
    logger.info("Text normalization completed.")
    return df


def deduplicate_posts(df: pd.DataFrame, threshold: float = 0.8) -> pd.DataFrame:
    """
    Deduplicates posts based on Jaccard similarity.

    Args:
        df (pd.DataFrame): The input DataFrame.
        threshold (float): The similarity threshold for deduplication.

    Returns:
        pd.DataFrame: The deduplicated DataFrame.
    """
    analyzer = SimilarityAnalyzer(text_column="post", threshold=threshold)
    df_clustered, _ = analyzer.run(df)
    return resolve_similarity_clusters(
        df_clustered, text_column="post", user_column="author"
    ).drop(columns=["similarity_cluster_id", "is_similarity_clustered"])


def remove_messy_posts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes posts that are considered 'polluted' (e.g., bot messages, ads, AI-generated).

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with polluted posts removed.
    """
    return PollutionFilter().filter_by_patterns(df, "all")


def normalize_post_size(
    df: pd.DataFrame, min_words: int = 300, max_chars: int = 10_000
) -> pd.DataFrame:
    """
    Filters posts based on quality criteria like length and non-alphanumeric ratio.

    Args:
        df (pd.DataFrame): The input DataFrame.
        min_words (int): Minimum number of words required in a post. Defaults to 300.
        max_chars (int): Maximum number of characters allowed in a post. Defaults to 10,000.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    initial_count = len(df)

    # Length filter
    df = df[
        (df["post"].str.split().str.len() >= min_words)
        & (df["post"].str.len() <= max_chars)
    ]
    logger.info(f"Filtered {initial_count - len(df)} posts based on length.")
    return df


def most_common_word_ratio(text: str) -> float:
    """
    Computes the ratio of the most frequent word in the text.

    Args:
        text (str): The input text.

    Returns:
        float: The ratio of the most frequent word's count to the total word count.
    """
    words = text.split()
    if not words:
        return 0.0
    word_counts = Counter(words)
    most_common_count = word_counts.most_common(1)[0][1]
    return most_common_count / len(words)


def non_standard_word_ratio(text: str) -> float:
    """
    Computes the ratio of non-alphanumeric words in the text.

    Args:
        text (str): The input text.

    Returns:
        float: The ratio of non-alphanumeric words to the total word count.
    """
    words = text.split()
    if not words:
        return 0.0
    return sum(1 for word in words if not word.isalnum()) / len(words)


def filter_by_non_standard_ratio(
    df: pd.DataFrame, threshold: float = 0.4
) -> pd.DataFrame:
    """
    Filters the DataFrame for posts that have a non-standard word ratio less than the threshold.

    Args:
        df (pd.DataFrame): The input DataFrame.
        threshold (float): Maximum allowed non-standard word ratio. Defaults to 0.4.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    df_filtered = df.copy()
    df_filtered["non_standard_word_ratio"] = df_filtered["post"].swifter.apply(
        non_standard_word_ratio
    )

    return df_filtered[df_filtered["non_standard_word_ratio"] < threshold].drop(
        columns=["non_standard_word_ratio"]
    )


def remove_common_word_by_ratio(
    df: pd.DataFrame, threshold: float = 0.3
) -> pd.DataFrame:
    """
    Removes the most common word from posts where its ratio exceeds the threshold.

    Args:
        df (pd.DataFrame): The input DataFrame.
        threshold (float): Minimum ratio to trigger removal. Defaults to 0.3.

    Returns:
        pd.DataFrame: The DataFrame with cleaned posts.
    """
    df_cleaned = df.copy()

    df_cleaned["most_common_word_ratio"] = df_cleaned["post"].swifter.apply(
        most_common_word_ratio
    )

    def sanitize_text(row: pd.Series) -> str:
        """
        Sanitizes a single row by removing the most common word if it exceeds the threshold.

        Args:
            row (pd.Series): The row containing 'post' and 'most_common_word_ratio'.

        Returns:
            str: The sanitized text.
        """
        if row["most_common_word_ratio"] > threshold:
            words = row["post"].split()
            if not words:
                return row["post"]

            word_counts = Counter(words)
            most_common_word = word_counts.most_common(1)[0][0]
            sanitized_words = [word for word in words if word != most_common_word]
            return " ".join(sanitized_words)
        return row["post"]

    df_cleaned["post"] = df_cleaned.swifter.apply(sanitize_text, axis=1)

    return df_cleaned.drop(columns=["most_common_word_ratio"])


class PreprocessingSteps(BaseModel):
    """
    Represents a series of preprocessing steps to be applied to a DataFrame.
    """

    steps: list[tuple[str, Callable[[pd.DataFrame], pd.DataFrame]]] = Field(
        default_factory=list
    )

    def run_steps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Executes all registered preprocessing steps.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The preprocessed DataFrame.
        """
        logger.info("Running preprocessing steps...")
        for step_name, step_func in self.steps:
            logger.info(f"Running step: {step_name}")
            df = step_func(df)
            logger.info(f"Step {step_name} completed.")
        logger.info("All steps completed.")
        return df


class DataPreprocessor(BasePipeline):
    """
    Preprocessing pipeline for raw data.

    Inherits from BasePipeline and applies a series of preprocessing steps.
    """

    # Use Pydantic Field with default_factory for dynamic defaults
    input_path: Path = Field(
        default_factory=lambda: ProjectPaths.political_leaning_csv()
    )
    output_path: Path = Field(default_factory=lambda: ProjectPaths.preprocessed_csv())

    preprocessing_steps: PreprocessingSteps = Field(
        default_factory=lambda: PreprocessingSteps(steps=[])
    )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply preprocessing steps to DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Preprocessed DataFrame
        """
        logger.info("Applying preprocessing steps...")
        df_preprocessed = self.preprocessing_steps.run_steps(df)
        logger.info("Preprocessing steps completed.")
        return df_preprocessed


def preprocess_data(
    data_url: Path = ProjectPaths.political_leaning_csv(),
    output_path: Path = ProjectPaths.preprocessed_csv(),
) -> None:
    """
    Main entry point for the preprocessing script.

    Args:
        data_url: Path to the raw data file (defaults to ProjectPaths.political_leaning_csv())
        output_path: Path where the preprocessed data will be saved (defaults to ProjectPaths.preprocessed_csv())
    """
    steps = [
        ("Fixing column typo", fix_typo),
        ("Cleaning the posts content", clean_content),
        ("Removing exact duplicates", remove_duplicates),
        ("Filter non-stylometric posts", filter_by_non_standard_ratio),
        ("Removing potentially messy posts", remove_messy_posts),
        ("Remove repetitive wording", remove_common_word_by_ratio),
        ("Filter for the post size", normalize_post_size),
        ("Deduplicating similar posts", deduplicate_posts),
    ]

    preprocessor = DataPreprocessor(
        input_path=data_url,
        output_path=output_path,
        preprocessing_steps=PreprocessingSteps(steps=steps),
    )
    preprocessor.run()


if __name__ == "__main__":
    preprocess_data()
