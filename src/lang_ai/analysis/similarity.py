"""
Text similarity analysis and clustering using character-level n-grams and Jaccard similarity.
"""

from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components
from sklearn.feature_extraction.text import CountVectorizer
from tqdm.auto import tqdm

from src.lang_ai.core.logger import setup_logging

logger = setup_logging(__name__)


class SimilarityAnalyzer(BaseModel):
    """
    Text similarity analyzer using character n-grams and Jaccard similarity.

    Identifies near-duplicate content (copypasta, repeated posts, heavily quoted
    material) using character-level n-grams and connected components clustering.
    Designed for efficient batch processing of large datasets.

    The analyzer works in two phases:
        1. Compute pairwise Jaccard similarity matrix (batched for memory efficiency)
        2. Identify connected components as similarity clusters

    Typical use case: Deduplication before stylometric analysis to prevent
    contamination from viral/repeated content.

    Attributes:
        text_column: Name of DataFrame column containing text to analyze
        threshold: Minimum Jaccard similarity to consider texts as duplicates (0.0-1.0)
        ngram_range: Tuple of (min_n, max_n) for character n-grams (default: 5-grams)
        analyzer: Analyzer type ('char_wb' for char with word boundaries)
        batch_size: Number of texts to process per batch (for memory management)
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    text_column: str = Field("post", description="Column containing text data")
    threshold: float = Field(
        0.8, description="Similarity threshold for clustering (0.0-1.0)", ge=0.0, le=1.0
    )
    ngram_range: tuple[int, int] = Field(
        (5, 5), description="Range of character n-grams (min, max)"
    )
    analyzer: str = Field(
        "char_wb", description="Analyzer type: 'char' or 'char_wb' (word boundaries)"
    )
    batch_size: int = Field(
        default=500, description="Batch size for memory-efficient processing", gt=0
    )

    _similarity_matrix: Any = PrivateAttr(default=None)

    def compute_similarity_matrix(self, df: pd.DataFrame) -> csr_matrix:
        """
        Compute sparse Jaccard similarity matrix using batched processing.

        Uses character n-grams and binary term-document matrix to compute
        Jaccard similarity efficiently in batches, only storing pairs that
        exceed the similarity threshold.

        Args:
            df: Input DataFrame containing text column

        Returns:
            Sparse CSR matrix of shape (n_texts, n_texts) containing Jaccard
            similarities ≥ threshold. Diagonal elements are not included.

        Note:
            - Processing is done in batches to avoid memory issues with large datasets
            - Only similarities above threshold are stored (sparse matrix)
            - Progress is tracked with tqdm progress bar
        """
        texts = df[self.text_column].fillna("").astype(str)
        n_texts = len(texts)

        logger.info(f"Vectorizing {n_texts} texts for similarity analysis...")
        vectorizer = CountVectorizer(
            analyzer=self.analyzer,
            ngram_range=self.ngram_range,
            binary=True,  # Jaccard set-based
            min_df=2,
        )

        X = vectorizer.fit_transform(texts)
        logger.info(f"Term-Document Matrix shape: {X.shape}")

        # Process in batches to avoid memory issues
        logger.info(f"Computing Jaccard similarity in batches of {self.batch_size}...")

        all_rows = []
        all_cols = []
        all_data = []

        doc_lengths = np.array(X.sum(axis=1)).flatten()

        # Create progress bar for batch processing
        n_batches = (n_texts + self.batch_size - 1) // self.batch_size

        with tqdm(total=n_batches, desc="Computing similarity", unit="batch") as pbar:
            for batch_start in range(0, n_texts, self.batch_size):
                batch_end = min(batch_start + self.batch_size, n_texts)

                # Compute intersection for this batch
                X_batch = X[batch_start:batch_end]
                intersection_batch = X_batch @ X.T
                intersection_batch = intersection_batch.tocoo()

                rows = intersection_batch.row + batch_start
                cols = intersection_batch.col
                data = intersection_batch.data

                # Compute union
                union_data = doc_lengths[rows] + doc_lengths[cols] - data

                # Compute Jaccard similarity
                with np.errstate(divide="ignore", invalid="ignore"):
                    jaccard_data = data / union_data
                    jaccard_data = np.nan_to_num(jaccard_data)

                # Filter by threshold and remove diagonal
                mask = (jaccard_data >= self.threshold) & (rows != cols)

                if mask.any():
                    all_rows.append(rows[mask])
                    all_cols.append(cols[mask])
                    all_data.append(jaccard_data[mask])

                pbar.update(1)

        # Combine all batches
        if all_rows:
            filtered_rows = np.concatenate(all_rows)
            filtered_cols = np.concatenate(all_cols)
            filtered_data = np.concatenate(all_data)
        else:
            filtered_rows = np.array([])
            filtered_cols = np.array([])
            filtered_data = np.array([])

        self._similarity_matrix = csr_matrix(
            (filtered_data, (filtered_rows, filtered_cols)), shape=(n_texts, n_texts)
        )

        logger.info(
            f"Similarity matrix computed. Found {self._similarity_matrix.nnz // 2} pairs above threshold {self.threshold}."
        )
        return self._similarity_matrix

    def identify_clusters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Identify connected components (clusters) in similarity graph.

        Uses the computed similarity matrix to find groups of texts that are
        transitively similar (if A~B and B~C, then A, B, C form a cluster).

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with added columns:
                - similarity_cluster_id: Integer cluster ID for each text
                - is_similarity_clustered: Boolean indicating if text belongs
                  to a multi-item cluster (True) or is unique (False)

        Raises:
            ValueError: If similarity matrix hasn't been computed yet
                (call compute_similarity_matrix first)

        Note:
            Texts with no similar matches are assigned unique cluster IDs
            but marked as is_similarity_clustered=False
        """
        if self._similarity_matrix is None:
            raise ValueError(
                "Compute similarity matrix first using compute_similarity_matrix()"
            )

        logger.info("Identifying similarity clusters via connected components...")

        n_components, labels = connected_components(
            csgraph=self._similarity_matrix, directed=False, return_labels=True
        )

        logger.info(f"Identified {n_components} connected components (clusters).")

        df_out = df.copy()
        df_out["similarity_cluster_id"] = labels

        # Filter for labels that appear more than once (actual clusters)
        cluster_counts = pd.Series(labels).value_counts()
        multi_item_clusters = set(cluster_counts[cluster_counts > 1].index)

        df_out["is_similarity_clustered"] = df_out["similarity_cluster_id"].isin(
            multi_item_clusters
        )

        clustered_count = df_out["is_similarity_clustered"].sum()
        logger.info(
            f"Marked {clustered_count} items as belonging to similarity clusters."
        )

        return df_out

    def run(self, df: pd.DataFrame) -> tuple[pd.DataFrame, csr_matrix]:
        """
        Execute complete similarity analysis pipeline.

        Convenience method that runs both matrix computation and cluster
        identification in sequence.

        Args:
            df: Input DataFrame with text column

        Returns:
            Tuple of:
                - DataFrame with cluster labels added
                - Sparse similarity matrix (CSR format)

        Example:
            >>> analyzer = SimilarityAnalyzer(text_column="post", threshold=0.8)
            >>> df_clustered, sim_matrix = analyzer.run(df)
            >>> # Use df_clustered for deduplication
        """
        matrix = self.compute_similarity_matrix(df)
        df_clustered = self.identify_clusters(df)
        return df_clustered, matrix


def resolve_similarity_clusters(
    df: pd.DataFrame, text_column: str = "post", user_column: str = "user_id"
) -> pd.DataFrame:
    """
    Resolve similarity clusters using stylometric best practices for deduplication.

    Implements a conservative deduplication strategy that preserves stylometric
    validity while removing problematic duplicates:

    Resolution Strategy:
        1. Multi-User Clusters (Viral/Copypasta):
           - Action: DROP all posts in cluster
           - Reason: Ambiguous authorship - cannot safely attribute style
           - Examples: Copypasta, viral quotes, bot-posted templates

        2. Single-User Clusters (Self-Reposts):
           - Action: KEEP only the longest post
           - Reason: Longest version has most stylometric signal
           - Examples: User posting same content to multiple subreddits

        3. Non-Clustered Posts:
           - Action: KEEP as-is
           - Reason: No similarity issues detected

    Args:
        df: Input DataFrame with similarity clustering columns added
            (must have 'similarity_cluster_id' and 'is_similarity_clustered')
        text_column: Name of column containing post text (default: "post")
        user_column: Name of column containing user/author ID (default: "user_id")

    Returns:
        Deduplicated DataFrame with cluster columns removed

    Raises:
        ValueError: If required clustering columns are missing
            (run SimilarityAnalyzer first)

    Example:
        >>> analyzer = SimilarityAnalyzer(threshold=0.8)
        >>> df_clustered, _ = analyzer.run(df)
        >>> df_clean = resolve_similarity_clusters(df_clustered, user_column="author")

    Note:
        This function removes the cluster ID columns from the output.
        Call this after SimilarityAnalyzer.run() or .identify_clusters()
    """
    if (
        "similarity_cluster_id" not in df.columns
        or "is_similarity_clustered" not in df.columns
    ):
        raise ValueError(
            "DataFrame must contain clustering columns. Run SimilarityAnalyzer first."
        )

    # Separate clustered and unclustered data
    is_clustered = df["is_similarity_clustered"]
    df_clean = df[~is_clustered].copy()
    df_clustered = df[is_clustered].copy()

    initial_count = len(df)
    n_clusters = df_clustered["similarity_cluster_id"].nunique()
    logger.info(f"Starting resolution of {n_clusters} clusters...")

    kept_posts = []

    # Group by cluster ID to process each similarity group with progress bar
    grouped = df_clustered.groupby("similarity_cluster_id")

    with tqdm(total=n_clusters, desc="Resolving clusters", unit="cluster") as pbar:
        for cluster_id, group in grouped:
            unique_users = group[user_column].nunique()

            if unique_users > 1:
                # SCENARIO 1: Multi-User Cluster (Viral Content / Copypasta)
                # Action: Drop entire cluster.
                logger.debug(
                    f"Cluster {cluster_id}: Dropped viral content shared by {unique_users} users."
                )
            else:
                # SCENARIO 2: Single-User Cluster (Self-Repetition)
                # Action: Keep only the longest post.
                max_len_idx = group[text_column].str.len().idxmax()
                best_post = group.loc[max_len_idx]
                kept_posts.append(best_post)

            pbar.update(1)

    # Combine cleaned data with the kept representatives from clusters
    if kept_posts:
        df_kept_clustered = pd.DataFrame(kept_posts)
        df_final = pd.concat([df_clean, df_kept_clustered], axis=0, ignore_index=True)
    else:
        df_final = df_clean

    dropped_count = initial_count - len(df_final)
    logger.info(
        f"Filtering complete. Dropped {dropped_count} rows. Final dataset size: {len(df_final)}."
    )

    return df_final
