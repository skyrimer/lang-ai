"""
Metrics module for deleaking evaluation pipeline.

Provides geometric analysis, stylometric analysis, and feature comparison utilities.
"""

from itertools import combinations
from typing import Dict, List, Set

import numpy as np
import spacy
from scipy.sparse import csr_matrix
from scipy.spatial.distance import euclidean
from sklearn.metrics import silhouette_score


class GeometricAnalyzer:
    """Analyzes geometric properties of feature space representations."""

    @staticmethod
    def compute_centroid_distances(X: csr_matrix, y: np.ndarray) -> Dict[str, float]:
        """
        Compute mean pairwise Euclidean distance between class centroids.

        Args:
            X: Sparse feature matrix (n_samples, n_features)
            y: Class labels (n_samples,)

        Returns:
            Dictionary with centroid distance metrics
        """
        unique_classes = np.unique(y)
        centroids = {}

        # Compute centroids for each class
        for cls in unique_classes:
            mask = y == cls
            class_samples = X[mask]
            # Mean across samples, maintaining sparse format
            centroid = np.asarray(class_samples.mean(axis=0)).flatten()
            centroids[cls] = centroid

        # Compute all pairwise distances
        distances = []
        class_pairs = []
        for cls1, cls2 in combinations(unique_classes, 2):
            dist = euclidean(centroids[cls1], centroids[cls2])
            distances.append(dist)
            class_pairs.append((cls1, cls2))

        return {
            "mean_centroid_distance": float(np.mean(distances)),
            "std_centroid_distance": float(np.std(distances)),
            "min_centroid_distance": float(np.min(distances)),
            "max_centroid_distance": float(np.max(distances)),
            "pairwise_distances": {
                f"{c1}_vs_{c2}": float(dist)
                for (c1, c2), dist in zip(class_pairs, distances)
            },
        }

    @staticmethod
    def compute_silhouette(
        X: csr_matrix, y: np.ndarray, sample_size: int = 5000
    ) -> float:
        """
        Compute silhouette score with sampling for large datasets.

        Args:
            X: Sparse feature matrix
            y: Class labels
            sample_size: Maximum samples to use (for computational efficiency)

        Returns:
            Silhouette score (-1 to 1, higher is better)
        """
        if X.shape[0] > sample_size:
            # Random sampling for efficiency
            indices = np.random.choice(X.shape[0], sample_size, replace=False)
            X_sample = X[indices]
            y_sample = y[indices]
        else:
            X_sample = X
            y_sample = y

        # Convert to dense for silhouette computation (required by sklearn)
        X_dense = X_sample.toarray()
        score = silhouette_score(X_dense, y_sample, metric="euclidean")

        return float(score)


class StylometricAnalyzer:
    """Analyzes stylometric properties of text features."""

    # Stylometric feature categories
    FUNCTION_WORDS = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "but",
        "if",
        "because",
        "as",
        "of",
        "at",
        "by",
        "for",
        "with",
        "about",
        "into",
        "through",
        "during",
        "before",
        "after",
        "above",
        "below",
        "to",
        "from",
        "up",
        "down",
        "in",
        "out",
        "on",
        "off",
        "over",
        "under",
        "again",
        "further",
        "then",
        "once",
        "here",
        "there",
        "when",
        "where",
        "why",
        "how",
        "all",
        "both",
        "each",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "no",
        "nor",
        "not",
        "only",
        "own",
        "same",
        "so",
        "than",
        "too",
        "very",
        "can",
        "will",
        "just",
        "should",
        "now",
        "i",
        "you",
        "he",
        "she",
        "it",
        "we",
        "they",
        "them",
        "their",
        "what",
        "which",
        "who",
        "this",
        "that",
        "these",
        "those",
        "am",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "do",
        "does",
        "did",
        "my",
        "your",
        "his",
        "her",
        "its",
        "our",
    }

    PUNCTUATION = {",", ".", "!", "?", ";", ":", "-", "--", "...", '"', "'"}

    def __init__(self):
        """Initialize with spaCy model for POS tagging."""
        try:
            self.nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        except OSError:
            raise RuntimeError(
                "spaCy model 'en_core_web_sm' not found. "
                "Install with: python -m spacy download en_core_web_sm"
            )

    def is_stylometric_feature(self, feature: str) -> bool:
        """
        Determine if a feature is stylometric (function word, punctuation, or adverb).

        Args:
            feature: Feature string from TF-IDF vocabulary

        Returns:
            True if feature is stylometric
        """
        # Check function words and punctuation
        if feature.lower() in self.FUNCTION_WORDS or feature in self.PUNCTUATION:
            return True

        # Check for adverbs using POS tagging
        doc = self.nlp(feature)
        if len(doc) > 0 and doc[0].pos_ == "ADV":
            return True

        return False

    def compute_stylometric_ratio(
        self, features: List[str], top_n: int = 50
    ) -> Dict[str, float]:
        """
        Compute stylometric ratio for top-N features.

        Args:
            features: List of feature strings
            top_n: Number of top features to analyze

        Returns:
            Dictionary with stylometric analysis
        """
        top_features = features[:top_n]
        stylometric_count = sum(
            1 for feat in top_features if self.is_stylometric_feature(feat)
        )

        # Breakdown by category
        function_words = sum(
            1 for f in top_features if f.lower() in self.FUNCTION_WORDS
        )
        punctuation = sum(1 for f in top_features if f in self.PUNCTUATION)

        # Adverbs (excluding already counted function words)
        adverbs = 0
        for feat in top_features:
            if feat.lower() not in self.FUNCTION_WORDS and feat not in self.PUNCTUATION:
                doc = self.nlp(feat)
                if len(doc) > 0 and doc[0].pos_ == "ADV":
                    adverbs += 1

        return {
            "stylometric_ratio": stylometric_count / top_n,
            "function_word_ratio": function_words / top_n,
            "punctuation_ratio": punctuation / top_n,
            "adverb_ratio": adverbs / top_n,
            "topic_ratio": (top_n - stylometric_count) / top_n,
            "total_features_analyzed": top_n,
        }


class FeatureComparator:
    """Compares feature sets between different corpora."""

    @staticmethod
    def jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
        """
        Compute Jaccard similarity between two feature sets.

        Args:
            set1: First feature set
            set2: Second feature set

        Returns:
            Jaccard similarity coefficient (0 to 1)
        """
        if len(set1) == 0 and len(set2) == 0:
            return 1.0

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union if union > 0 else 0.0

    @staticmethod
    def compare_feature_sets(
        baseline_features: Dict[str, List[str]],
        sanitized_features: Dict[str, List[str]],
        top_n: int = 50,
    ) -> Dict[str, Dict[str, float]]:
        """
        Compare feature sets between baseline and sanitized corpora.

        Args:
            baseline_features: Dict mapping class -> list of features
            sanitized_features: Dict mapping class -> list of features
            top_n: Number of top features to compare per class

        Returns:
            Dictionary with per-class and overall Jaccard similarities
        """
        results = {}
        jaccard_scores = []

        for cls in baseline_features:
            if cls not in sanitized_features:
                continue

            baseline_set = set(baseline_features[cls][:top_n])
            sanitized_set = set(sanitized_features[cls][:top_n])

            jaccard = FeatureComparator.jaccard_similarity(baseline_set, sanitized_set)
            results[f"class_{cls}"] = {
                "jaccard_similarity": jaccard,
                "baseline_count": len(baseline_set),
                "sanitized_count": len(sanitized_set),
                "intersection_count": len(baseline_set & sanitized_set),
            }
            jaccard_scores.append(jaccard)

        results["overall"] = {
            "mean_jaccard": float(np.mean(jaccard_scores)),
            "std_jaccard": float(np.std(jaccard_scores)),
            "min_jaccard": float(np.min(jaccard_scores)),
            "max_jaccard": float(np.max(jaccard_scores)),
        }

        return results
