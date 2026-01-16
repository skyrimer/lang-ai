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
    Compute the ratio of the most frequently occurring word in text.

    Used to detect repetitive or spam content where a single word
    dominates the text (e.g., "word word word...").

    Args:
        text: Input text string

    Returns:
        Ratio of the most common word's count to total word count (0.0-1.0).
        Returns 0.0 for empty text.

    Example:
        >>> most_common_word_ratio("hello hello world")
        0.666...  # "hello" appears 2 out of 3 times
    """
    words = text.split()
    if not words:
        return 0.0
    word_counts = Counter(words)
    most_common_count = word_counts.most_common(1)[0][1]
    return most_common_count / len(words)


def non_standard_word_ratio(text: str) -> float:
    """
    Compute the ratio of non-alphanumeric tokens in text.

    Used to detect noisy content with excessive special characters,
    emojis, or malformed text (e.g., "!@#$ *** %%%").

    Args:
        text: Input text string

    Returns:
        Ratio of non-alphanumeric words to total word count (0.0-1.0).
        Returns 0.0 for empty text.

    Example:
        >>> non_standard_word_ratio("hello !!! ??? world")
        0.5  # 2 out of 4 tokens are non-alphanumeric
    """
    words = text.split()
    if not words:
        return 0.0
    return sum(1 for word in words if not word.isalnum()) / len(words)


def filter_by_non_standard_ratio(
    df: pd.DataFrame, threshold: float = 0.4
) -> pd.DataFrame:
    """
    Filter posts by non-standard word ratio.

    Removes posts where more than threshold fraction of words are
    non-alphanumeric, indicating noisy or low-quality content.

    Args:
        df: Input DataFrame with 'post' column
        threshold: Maximum allowed ratio of non-alphanumeric words (default: 0.4)

    Returns:
        Filtered DataFrame excluding posts above threshold

    Example:
        With threshold=0.4, keeps posts where ≤40% of words are non-alphanumeric
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
    Sanitize repetitive posts by removing dominant words.

    If a single word appears more than threshold fraction of total words,
    remove all instances of that word. This helps filter spam or bot posts
    that repeat the same word excessively.

    Args:
        df: Input DataFrame with 'post' column
        threshold: Minimum ratio to trigger word removal (default: 0.3)

    Returns:
        DataFrame with sanitized posts

    Example:
        With threshold=0.3, if "SPAM" appears >30% of the time in a post,
        all instances of "SPAM" are removed from that post.
    """
    df_cleaned = df.copy()

    df_cleaned["most_common_word_ratio"] = df_cleaned["post"].swifter.apply(
        most_common_word_ratio
    )

    def sanitize_text(row: pd.Series) -> str:
        """
        Sanitize a single row by removing dominant word if above threshold.

        Args:
            row: Series containing 'post' and 'most_common_word_ratio' fields

        Returns:
            Sanitized text with dominant word removed, or original text if
            no word exceeds threshold
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
    Container for preprocessing pipeline steps.

    Manages an ordered sequence of preprocessing functions to be applied
    to a DataFrame, with logging and progress tracking.
    """

    steps: list[tuple[str, Callable[[pd.DataFrame], pd.DataFrame]]] = Field(
        default_factory=list, description="List of (name, function) tuples"
    )

    def run_steps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute all registered preprocessing steps in sequence.

        Args:
            df: Input DataFrame

        Returns:
            Preprocessed DataFrame after applying all steps

        Note:
            Each step logs its name before and after execution
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
    Preprocessing pipeline for stylometric analysis.

    Applies comprehensive data cleaning to prepare Reddit posts for
    authorship attribution by removing pollution (bots, ads, AI-generated),
    normalizing text, deduplicating similar content, and enforcing quality
    constraints while preserving stylometric signals.

    Pipeline steps:
        1. Fix column typos
        2. Clean content (normalize URLs, emails, etc.)
        3. Remove exact duplicates
        4. Filter by non-standard word ratio
        5. Remove messy posts (bots, ads, AI)
        6. Remove repetitive wording
        7. Normalize post size
        8. Deduplicate similar posts

    Attributes:
        input_path: Path to raw data file
        output_path: Path to save preprocessed data
        preprocessing_steps: Ordered sequence of preprocessing functions
    """

    # Use Pydantic Field with default_factory for dynamic defaults
    input_path: Path | str = Field(
        default_factory=lambda: ProjectPaths.political_leaning_csv(),
        description="Path to raw input data",
    )
    output_path: Path | str = Field(
        default_factory=lambda: ProjectPaths.preprocessed_csv(),
        description="Path to save preprocessed output",
    )

    preprocessing_steps: PreprocessingSteps = Field(
        default_factory=lambda: PreprocessingSteps(steps=[]),
        description="Container for preprocessing step functions",
    )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all preprocessing steps to DataFrame.

        Args:
            df: Raw input DataFrame

        Returns:
            Cleaned and preprocessed DataFrame ready for analysis

        Note:
            Logs shape changes and statistics after each step
        """
        logger.info("Applying preprocessing steps...")
        df_preprocessed = self.preprocessing_steps.run_steps(df)
        logger.info("Preprocessing steps completed.")
        return df_preprocessed


def preprocess_data(
    data_url: Path | str = ProjectPaths.political_leaning_csv(),
    output_path: Path | str = ProjectPaths.preprocessed_csv(),
) -> None:
    """
    Execute the full preprocessing pipeline.

    Main entry point that instantiates and runs the preprocessing pipeline
    with standard steps for stylometric analysis preparation.

    Args:
        data_url: Path to raw data file (default: ProjectPaths.political_leaning_csv())
        output_path: Path to save preprocessed data (default: ProjectPaths.preprocessed_csv())

    Pipeline Steps:
        1. Fix column typos (e.g., auhtor_ID -> author)
        2. Clean content (normalize URLs, emails, mentions)
        3. Remove exact duplicate posts
        4. Filter by non-standard word ratio (<40% special chars)
        5. Remove messy posts (bots, ads, AI-generated)
        6. Remove repetitive wording (single word >30% of content)
        7. Filter by post size (min 300 words, max 10k chars)
        8. Deduplicate similar posts (Jaccard >0.8)

    Note:
        See individual step functions for detailed documentation of each filter
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
