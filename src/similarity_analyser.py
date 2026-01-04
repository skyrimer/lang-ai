from typing import Any
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components
from sklearn.feature_extraction.text import CountVectorizer
from pydantic import BaseModel, ConfigDict, PrivateAttr, Field
from dlm_logger import setup_logging
from tqdm import tqdm

logger = setup_logging()


class SimilarityAnalyzer(BaseModel):
    """
    Analyzes text similarity in a DataFrame using Jaccard similarity on character n-grams.
    Identifies clusters of near-duplicate or heavily quoted content.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    text_column: str = "post"
    threshold: float = 0.8
    ngram_range: tuple[int, int] = (5, 5)
    analyzer: str = "char_wb"  # Intra-word boundaries
    batch_size: int = Field(
        default=500, description="Number of texts to process per batch"
    )

    _similarity_matrix: Any = PrivateAttr(default=None)

    def compute_similarity_matrix(self, df: pd.DataFrame) -> csr_matrix:
        """
        Computes the sparse Jaccard similarity matrix for the text column in batches.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            csr_matrix: The computed sparse similarity matrix.
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
        Identifies connected components (clusters) in the similarity graph.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with 'similarity_cluster_id' and 'is_similarity_clustered' columns.
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
        Runs the full similarity analysis pipeline: computing matrix and identifying clusters.

        Args:
            df (pd.DataFrame): The input DataFrame.

        Returns:
            tuple[pd.DataFrame, csr_matrix]: The clustered DataFrame and the similarity matrix.
        """
        matrix = self.compute_similarity_matrix(df)
        df_clustered = self.identify_clusters(df)
        return df_clustered, matrix


def resolve_similarity_clusters(
    df: pd.DataFrame, text_column: str = "post", user_column: str = "user_id"
) -> pd.DataFrame:
    """
    Filters the clustered DataFrame based on stylometric best practices.

    1. Multi-User Clusters (Viral/Copypasta) -> Drop ALL posts in cluster.
       Reason: Cannot safely attribute authorship if multiple users post the same text.
    2. Single-User Clusters (Self-Reposts) -> Keep the LONGEST post.
       Reason: Preserves the instance with the most stylometric signal.
    3. Non-Clustered Posts -> Keep as is.

    Args:
        df (pd.DataFrame): The DataFrame with similarity clusters.
        text_column (str): The name of the column containing the text.
        user_column (str): The name of the column containing the user ID.

    Returns:
        pd.DataFrame: The resolved DataFrame.
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
