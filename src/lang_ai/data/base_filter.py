"""
Abstract base class for pattern-based text filtering.

Provides common infrastructure for filters that apply regex patterns
to DataFrames, either removing rows or replacing matched text.
"""

import re
from abc import ABC, abstractmethod
from typing import Dict, List, Literal, Optional, Tuple

import pandas as pd
from pydantic import BaseModel, Field
from tqdm.auto import tqdm

from src.lang_ai.core.logger import setup_logging

logger = setup_logging(__name__)


class TextFilter(BaseModel, ABC):
    """
    Abstract base for pattern-based text filtering.

    Subclasses must implement:
    - get_filter_patterns(): Return dict of category -> list of terms or regex pattern
    - get_replacement_token(): Return replacement token or None to remove rows

    Common functionality provided:
    - Pattern building from term lists
    - Batch filtering with logging and progress tracking
    - Statistics tracking
    """

    text_column: str = Field(
        default="post", description="Name of the column containing text to filter"
    )

    @abstractmethod
    def get_filter_patterns(self) -> Dict[str, List[str] | str]:
        """
        Return filter patterns for each category.

        Returns:
            Dictionary mapping category name to either:
            - List[str]: Terms to build regex from (will be escaped)
            - str: Pre-built regex pattern (will be used as-is)

        Example:
            {
                "bot": r"(?i)(I am a bot|contact the moderators)",
                "nicknames": ["Sleepy Joe", "Crooked Hillary"]
            }
        """
        pass

    @abstractmethod
    def get_replacement_token(self, category: str) -> Optional[str]:
        """
        Return replacement token for a category.

        Args:
            category: The filter category

        Returns:
            - str: Token to replace matches with (e.g., "[POLITICAL_NAME]")
            - None: Remove rows with matches instead of replacing

        Example:
            For PollutionFilter: return None (remove rows)
            For LeakageFilter: return "[POLITICAL_NAME]" (replace text)
        """
        pass

    def get_category_label(self, category: str) -> str:
        """
        Get human-readable label for category (for logging).

        Override to provide custom labels.

        Args:
            category: The filter category

        Returns:
            Human-readable label (defaults to title-cased category)
        """
        return category.replace("_", " ").title()

    def _build_pattern_from_terms(self, terms: List[str]) -> str:
        """
        Build regex pattern from a list of terms.

        Args:
            terms: List of terms to match

        Returns:
            Compiled regex pattern with word boundaries and case-insensitive flag

        Note:
            - Sorts terms by length (descending) to match longer phrases first
            - Escapes special regex characters
            - Adds word boundaries for exact matching
        """
        # Sort by length (descending) to match longer phrases first
        sorted_terms = sorted(terms, key=len, reverse=True)

        # Escape special regex characters and join with OR
        escaped_terms = [re.escape(term) for term in sorted_terms]

        return r"(?i)\b(?:" + "|".join(escaped_terms) + r")\b"

    def _get_compiled_pattern(self, category: str) -> str:
        """
        Get compiled regex pattern for a category.

        Args:
            category: The filter category

        Returns:
            Regex pattern string
        """
        patterns = self.get_filter_patterns()

        if category not in patterns:
            raise ValueError(
                f"Category '{category}' not found in filter patterns. "
                f"Available categories: {list(patterns.keys())}"
            )

        pattern_spec = patterns[category]

        # If pattern is already a string, use as-is
        if isinstance(pattern_spec, str):
            return pattern_spec

        # Otherwise, build pattern from term list
        return self._build_pattern_from_terms(pattern_spec)

    def apply_filter(
        self,
        df: pd.DataFrame,
        categories: List[str] | Literal["all"],
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """
        Apply filters to DataFrame.

        Args:
            df: Input DataFrame containing text column.
            categories: List of category names to filter, or "all" for all categories defined in get_filter_patterns().

        Returns:
            Tuple of (filtered_df, statistics_dict)
            - filtered_df: DataFrame with filters applied.
            - statistics_dict: Category labels -> count of matches/rows removed.

        Raises:
            ValueError: If a specified category is not found in patterns.
            KeyError: If the configured text_column is not found in the DataFrame.
        """
        if self.text_column not in df.columns:
            raise KeyError(
                f"Text column '{self.text_column}' not found in DataFrame. "
                f"Available columns: {list(df.columns)}"
            )

        df_filtered = df.copy()
        stats = {}

        # Resolve "all" to list of all categories
        if categories == "all":
            categories = list(self.get_filter_patterns().keys())

        logger.info(f"Applying filters for categories: {categories}")

        for category in tqdm(categories, desc="Filtering"):
            pattern = self._get_compiled_pattern(category)
            token = self.get_replacement_token(category)
            label = self.get_category_label(category)

            logger.info(f"Processing {label}...")

            # Count matches before filtering
            matches_count = df_filtered[self.text_column].str.count(pattern).sum()

            if token is None:
                # Remove rows with matches
                df_filtered = df_filtered[
                    ~df_filtered[self.text_column].str.contains(
                        pattern, regex=True, na=False
                    )
                ]
                logger.info(f"Removed {matches_count} rows matching {label}")
            else:
                # Replace matches with token
                df_filtered[self.text_column] = df_filtered[
                    self.text_column
                ].str.replace(pattern, token, regex=True)
                logger.info(f"Replaced {matches_count} instances of {label}")

            stats[label] = int(matches_count)

        logger.info(
            f"Filtering complete. Processed {len(categories)} categories. "
            f"Rows: {len(df)} → {len(df_filtered)}"
        )

        return df_filtered, stats

    def apply_single_category(
        self,
        df: pd.DataFrame,
        category: str,
    ) -> Tuple[pd.DataFrame, int]:
        """
        Apply filter for a single category.

        Args:
            df: Input DataFrame
            category: Category name to filter

        Returns:
            Tuple of (filtered_df, match_count)

        Raises:
            ValueError: If category not found
        """
        df_filtered, stats = self.apply_filter(df, [category])
        label = self.get_category_label(category)
        return df_filtered, stats.get(label, 0)

    def apply_all(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """
        Apply all filters to DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Tuple of (filtered_df, statistics_dict)
        """
        return self.apply_filter(df, "all")
