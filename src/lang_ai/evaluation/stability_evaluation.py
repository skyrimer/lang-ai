"""
Deleaking Assessment Pipeline (Cross-Validation Enabled).

This script implements a comparative evaluation pipeline using Stratified Group K-Fold
Cross Validation. It assesses the stability and effectiveness of dataset de-leaking.

Methodology:
1. Stratified Group K-Fold: Ensures robust performance estimation respecting author groups.
2. Stability Analysis:
   - Feature Stability Matrix: Measures Mean & Std of Jaccard overlap for various Top-N thresholds
     (aggregated across all classes).
   - Performance Stability: Mean and Std Dev of Macro F1.
3. Comparative Leakage Audit: Compares the 'Consensus Features' of Baseline vs. Deleaked.
4. Optimized Runtime: Uses a single Tokenizer per dataset to speed up validation.
"""

from collections import Counter
from itertools import combinations
from pathlib import Path
from typing import Dict, List

import nltk
import numpy as np
import pandas as pd
from omegaconf import DictConfig

# Scikit-learn imports
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.multiclass import OneVsRestClassifier
from sklearn.svm import LinearSVC

from src.lang_ai.core.logger import setup_logging

logger = setup_logging()

# Ensure NLTK resources
try:
    nltk.data.find("taggers/averaged_perceptron_tagger")
except LookupError:
    logger.info("Downloading NLTK POS tagger...")
    nltk.download("averaged_perceptron_tagger")
    nltk.download("averaged_perceptron_tagger_eng")
    nltk.download("punkt")

# --- Configuration ---

TEXT_COL = "post"
LABEL_COL = "political_leaning"
AUTHOR_COL = "author"

MODEL_CLASSES = {
    "LogisticRegression": LogisticRegression,
    "LinearSVC": LinearSVC,
}


class CVResult:
    """
    Container for Cross-Validation Results.

    Stores metrics, feature sets, and stability statistics across CV folds
    for a single dataset (baseline or deleaked).

    Attributes:
        name: Dataset identifier (e.g., "Baseline", "Deleaked")
        fold_metrics: Performance metrics for each CV fold
        fold_features: Top features extracted from each CV fold per class
        mean_metrics: Mean performance metrics across all folds
        std_metrics: Standard deviation of performance metrics across folds
        feature_stability_matrix: DataFrame containing mean/std Jaccard overlap
            for various Top-N thresholds aggregated across all classes
        consensus_features: Features appearing in majority of folds (>50%)
            per class, indicating stable discriminative features
    """

    def __init__(self, name: str) -> None:
        """
        Initialize CV results container.

        Args:
            name: Dataset identifier (e.g., "Baseline", "Deleaked")
        """
        self.name = name

        # Raw results per fold
        self.fold_metrics: List[Dict[str, float]] = []
        self.fold_features: List[
            Dict[str, List[str]]
        ] = []  # List of {class: [features]}

        # Aggregated Statistics
        self.mean_metrics: Dict[str, float] = {}
        self.std_metrics: Dict[str, float] = {}

        # Stability Data
        self.feature_stability_matrix: pd.DataFrame = (
            pd.DataFrame()
        )  # Matrix of Mean/Std overlap per N
        self.consensus_features: Dict[
            str, List[str]
        ] = {}  # Class -> Features present in >50% folds


class FeatureStabilityAnalyzer:
    """
    Analyzes consistency of feature selection across cross-validation folds.

    Provides methods to quantify feature stability using Jaccard similarity
    and identify consensus features that appear consistently across folds.
    """

    @staticmethod
    def get_pairwise_jaccard_scores(feature_lists: List[List[str]]) -> List[float]:
        """
        Calculate Jaccard indices between all pairs of feature lists.

        Args:
            feature_lists: List of feature lists to compare

        Returns:
            List of Jaccard similarity scores for all pairwise combinations
        """
        if len(feature_lists) < 2:
            return []

        scores = []
        for list_a, list_b in combinations(feature_lists, 2):
            set_a = set(list_a)
            set_b = set(list_b)
            union = len(set_a.union(set_b))
            intersect = len(set_a.intersection(set_b))
            scores.append(intersect / union if union > 0 else 0.0)

        return scores

    @staticmethod
    def compute_stability_matrix(
        fold_features_list: List[Dict[str, List[str]]], n_list: List[int]
    ) -> pd.DataFrame:
        """
        Generate feature stability statistics for various Top-N thresholds.

        Computes mean and standard deviation of Jaccard overlap between
        feature sets across all CV folds, aggregated across all classes.

        Args:
            fold_features_list: List of dictionaries mapping class to feature list for each fold
            n_list: List of Top-N thresholds to analyze (e.g., [5, 10, 20, 50])

        Returns:
            DataFrame with columns: Top_N, Mean_Overlap, Std_Overlap
        """
        records = []
        if not fold_features_list:
            return pd.DataFrame()

        classes = fold_features_list[0].keys()

        for n in n_list:
            all_scores_for_n = []

            # Aggregate pairwise scores from ALL classes
            for cls in classes:
                # Get lists for this class across all folds
                full_lists = [f.get(cls, []) for f in fold_features_list]
                # Slice to Top N
                sliced_lists = [class_list[:n] for class_list in full_lists]

                # Compute pairwise scores
                scores = FeatureStabilityAnalyzer.get_pairwise_jaccard_scores(
                    sliced_lists
                )
                all_scores_for_n.extend(scores)

            # Calculate stats over the entire population of pairwise comparisons
            if all_scores_for_n:
                mean_overlap = np.mean(all_scores_for_n)
                std_overlap = np.std(all_scores_for_n)
            else:
                mean_overlap = 0.0
                std_overlap = 0.0

            records.append(
                {"Top_N": n, "Mean_Overlap": mean_overlap, "Std_Overlap": std_overlap}
            )

        return pd.DataFrame(records)

    @staticmethod
    def extract_consensus_features(
        fold_features_list: List[Dict[str, List[str]]], min_folds: int = 2
    ) -> Dict[str, List[str]]:
        """
        Identify features that appear consistently across multiple CV folds.

        Args:
            fold_features_list: List of dictionaries mapping class to feature list for each fold
            min_folds: Minimum number of folds a feature must appear in to be considered consensus

        Returns:
            Dictionary mapping class label to list of consensus features,
            sorted by frequency (descending) then alphabetically
        """
        consensus = {}
        if not fold_features_list:
            return {}

        classes = fold_features_list[0].keys()

        for cls in classes:
            all_feats = []
            for fold_res in fold_features_list:
                all_feats.extend(fold_res.get(cls, []))

            counts = Counter(all_feats)
            stable_feats = [
                feat for feat, count in counts.items() if count >= min_folds
            ]
            stable_feats.sort(key=lambda x: (-counts[x], x))
            consensus[cls] = stable_feats

        return consensus


class LeakageQuantifier:
    """
    Quantifies leakage removal effectiveness by comparing feature sets.

    Computes Jaccard overlap between baseline and deleaked consensus features
    per class at various Top-N thresholds. Lower overlap indicates more
    successful leakage removal (features have shifted from political content
    to stylistic markers).
    """

    def __init__(
        self, baseline: CVResult, deleaked: CVResult, top_n_list: List[int]
    ) -> None:
        """
        Initialize leakage quantifier.

        Args:
            baseline: CV results for baseline (preprocessed) dataset
            deleaked: CV results for deleaked (sanitized) dataset
            top_n_list: List of Top-N thresholds to analyze
        """
        self.base = baseline
        self.clean = deleaked
        self.classes = list(baseline.consensus_features.keys())
        self.top_n_list = top_n_list

    def analyze(self) -> pd.DataFrame:
        """
        Create comparison table showing Jaccard overlap per class and Top-N.

        Returns:
            DataFrame with columns: Class, Top_5, Top_10, Top_20, etc.
            Each cell contains Jaccard similarity (0.0-1.0) between baseline
            and deleaked feature sets for that class at that threshold.
        """
        rows = []

        for cls in self.classes:
            base_feats = self.base.consensus_features.get(cls, [])
            clean_feats = self.clean.consensus_features.get(cls, [])

            row = {"Class": cls}

            for n in self.top_n_list:
                set_base = set(base_feats[:n])
                set_clean = set(clean_feats[:n])
                union = len(set_base.union(set_clean))
                jaccard = (
                    len(set_base.intersection(set_clean)) / union if union > 0 else 0.0
                )
                row[f"Top_{n}"] = round(jaccard, 3)

            rows.append(row)

        return pd.DataFrame(rows)


class EvaluationPipeline:
    """
    Cross-validation pipeline for deleaking assessment.

    Implements Stratified Group K-Fold CV to evaluate the stability and
    effectiveness of dataset de-leaking while respecting author groups.
    """

    def __init__(self, config: DictConfig) -> None:
        """
        Initialize evaluation pipeline with configuration.

        Args:
            config: Hydra configuration object containing pipeline settings,
                model parameters, and feature analysis thresholds
        """
        self.cfg = config
        # Default N list if not provided
        self.top_n_list = config.feature_analysis.top_n_list

    def load_data(self, path: Path | str) -> pd.DataFrame:
        """
        Load and validate dataset from CSV or JSON file.

        Args:
            path: Path to the data file

        Returns:
            Cleaned DataFrame with required columns (post, political_leaning, author)

        Raises:
            ValueError: If file format is unsupported or required columns are missing
        """
        path = Path(path)
        logger.info(f"Loading data from {path}")
        if path.suffix == ".csv":
            df = pd.read_csv(path)
        elif path.suffix == ".json":
            df = pd.read_json(path, lines=True)
        else:
            raise ValueError(f"Unsupported file format: {path.suffix}")

        cols = [TEXT_COL, LABEL_COL, AUTHOR_COL]
        df = df.dropna(subset=cols)
        return df

    def extract_top_features(
        self, vectorizer: TfidfVectorizer, model, classes: np.ndarray, n_limit: int
    ) -> Dict[str, List[str]]:
        """
        Extract top features from a trained model for each class.

        Args:
            vectorizer: Fitted TfidfVectorizer instance.
            model: Trained scikit-learn model.
            classes: Array of class labels.
            n_limit: Number of top features to extract per class.

        Returns:
            Dictionary mapping class label to list of top features.
        """
        feature_names = vectorizer.get_feature_names_out()
        top_feats = {}

        if isinstance(model, OneVsRestClassifier):
            estimators = model.estimators_
            for idx, cls in enumerate(classes):
                coefs = estimators[idx].coef_.flatten()
                # Get indices for top N features
                top_indices = np.argsort(coefs)[-n_limit:][::-1]
                top_feats[str(cls)] = [feature_names[i] for i in top_indices]
        else:
            # Assuming LinearSVC or LogisticRegression native multiclass
            for idx, cls in enumerate(classes):
                coefs = model.coef_[idx]
                top_indices = np.argsort(coefs)[-n_limit:][::-1]
                top_feats[str(cls)] = [feature_names[i] for i in top_indices]
        return top_feats

    def run_cv(
        self, data_path: Path | str, dataset_name: str, n_splits: int = 5
    ) -> CVResult:
        """
        Execute Stratified Group K-Fold Cross-Validation pipeline.

        Performs the following steps:
        1. Load and vectorize data (single vectorizer for speed)
        2. Split data using Stratified Group K-Fold (respects author groups)
        3. Train models and collect metrics for each fold
        4. Extract top-N features from reference model
        5. Compute aggregated statistics and feature stability

        Args:
            data_path: Path to the dataset file (CSV or JSON)
            dataset_name: Identifier for this dataset (e.g., "Baseline", "Deleaked")
            n_splits: Number of cross-validation folds (default: 5)

        Returns:
            CVResult containing fold-wise metrics, consensus features,
            and feature stability analysis
        """
        data_path = Path(data_path)
        logger.info(f"--- Starting CV Pipeline: {dataset_name} ({n_splits} folds) ---")

        df = self.load_data(data_path)
        result = CVResult(dataset_name)

        # 1. Single Vectorization (Speed Optimization)
        logger.info("Fitting vectorizer on full dataset...")
        tokenizer_cfg = dict(self.cfg.tokenizer)
        vectorizer = TfidfVectorizer(
            ngram_range=tuple(tokenizer_cfg.pop("ngram_range")), **tokenizer_cfg
        )

        X_raw = df[TEXT_COL].values
        # Transform ALL data once
        X_all_vec = vectorizer.fit_transform(X_raw)

        y_raw = df[LABEL_COL].values
        groups = df[AUTHOR_COL].values

        # Setup CV
        cv = StratifiedGroupKFold(n_splits=n_splits)
        f1_scores = {name: [] for name in self.cfg.models.keys()}

        # We need to extract enough features to cover the largest N requested
        max_n = max(self.top_n_list)

        for fold_idx, (train_idx, val_idx) in enumerate(
            cv.split(X_raw, y_raw, groups=groups)
        ):
            logger.info(f"Processing Fold {fold_idx + 1}/{n_splits}...")

            # Slice the pre-computed sparse matrix
            X_train_vec = X_all_vec[train_idx]
            X_val_vec = X_all_vec[val_idx]
            y_train_fold, y_val_fold = y_raw[train_idx], y_raw[val_idx]

            classes = np.unique(y_train_fold)
            current_fold_features = {}

            # Train Models
            for i, (name, model_config) in enumerate(self.cfg.models.items()):
                model_cls = MODEL_CLASSES[model_config["class"]]
                model = model_cls(**dict(model_config["params"]))

                model.fit(X_train_vec, y_train_fold)
                preds = model.predict(X_val_vec)

                score = f1_score(y_val_fold, preds, average="macro")
                f1_scores[name].append(score)

                # Extract features from the first model only (Reference model)
                if i == 0:
                    current_fold_features = self.extract_top_features(
                        vectorizer, model, classes, max_n
                    )

            result.fold_features.append(current_fold_features)

        # --- Aggregate Statistics ---

        # 1. Metrics Stats
        for name, scores in f1_scores.items():
            result.mean_metrics[f"{name}_mean_f1"] = np.mean(scores)
            result.std_metrics[f"{name}_std_f1"] = np.std(scores)
            logger.info(
                f"{name}: Mean F1 = {np.mean(scores):.4f} (+/- {np.std(scores):.4f})"
            )

        # 2. Consensus Features
        min_folds = (n_splits // 2) + 1
        result.consensus_features = FeatureStabilityAnalyzer.extract_consensus_features(
            result.fold_features, min_folds=min_folds
        )

        # 3. Feature Stability Matrix (Mean/Std for list of Ns)
        logger.info("Computing Feature Stability Matrix...")
        result.feature_stability_matrix = (
            FeatureStabilityAnalyzer.compute_stability_matrix(
                result.fold_features, self.top_n_list
            )
        )

        return result


def main(cfg: DictConfig) -> None:
    """
    Main entry point for cross-validation deleaking assessment.

    Runs stratified group K-fold CV on both baseline and deleaked datasets,
    compares their feature stability and performance, and generates
    comprehensive comparison reports.

    Args:
        cfg: Hydra configuration object containing:
            - paths.baseline: Path to baseline (preprocessed) dataset
            - paths.sanitized: Path to deleaked (sanitized) dataset
            - paths.output_dir: Directory for saving results
            - cv_splits: Number of CV folds
            - models: Model configurations
            - feature_analysis: Top-N thresholds for analysis

    Outputs:
        - comparative_feature_stability.csv: Feature stability comparison
        - cv_leakage_quantification.csv: Per-class leakage metrics
        - Console logs with performance and stability statistics
    """
    baseline_path = Path(cfg.paths.baseline)
    sanitized_path = Path(cfg.paths.sanitized)
    output_dir = Path(cfg.paths.output_dir)
    n_splits = cfg.cv_splits

    pipeline = EvaluationPipeline(cfg)

    # 1. Run CV on BASELINE
    res_baseline = pipeline.run_cv(baseline_path, "Baseline", n_splits)

    # 2. Run CV on DELEAKED
    res_deleaked = pipeline.run_cv(sanitized_path, "Deleaked", n_splits)

    # 3. Comparative Analysis
    quantifier = LeakageQuantifier(
        res_baseline, res_deleaked, cfg.feature_analysis.top_n_list
    )
    leakage_df = quantifier.analyze()

    # --- OUTPUT REPORTS ---
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("CROSS-VALIDATION DELEAKING REPORT")

    model_name = list(cfg.models.keys())[0]

    # 1. Performance Stability
    logger.info(f"\n1. PERFORMANCE STABILITY ({n_splits}-Fold CV)")
    logger.info(
        f"{'Metric':<20} | {'Baseline (Mean +/- Std)':<30} | {'Deleaked (Mean +/- Std)':<30}"
    )

    base_m = res_baseline.mean_metrics[f"{model_name}_mean_f1"]
    base_s = res_baseline.std_metrics[f"{model_name}_std_f1"]
    del_m = res_deleaked.mean_metrics[f"{model_name}_mean_f1"]
    del_s = res_deleaked.std_metrics[f"{model_name}_std_f1"]

    logger.info(
        f"{'Macro F1':<20} | {base_m:.4f} +/- {base_s:.4f}           | {del_m:.4f} +/- {del_s:.4f}"
    )

    # 2. Feature Stability Matrix
    logger.info("\n2. FEATURE STABILITY MATRIX (Mean Jaccard +/- Std)")
    logger.info("Aggregated across all classes.")

    if (
        not res_baseline.feature_stability_matrix.empty
        and not res_deleaked.feature_stability_matrix.empty
    ):
        stab_base = res_baseline.feature_stability_matrix.set_index("Top_N")
        stab_clean = res_deleaked.feature_stability_matrix.set_index("Top_N")

        stab_base = stab_base.rename(
            columns={"Mean_Overlap": "Base_Mean", "Std_Overlap": "Base_Std"}
        )
        stab_clean = stab_clean.rename(
            columns={"Mean_Overlap": "Clean_Mean", "Std_Overlap": "Clean_Std"}
        )

        merged = stab_base.join(stab_clean)
        merged["Baseline"] = merged.apply(
            lambda x: f"{x['Base_Mean']:.3f} ± {x['Base_Std']:.3f}", axis=1
        )
        merged["Deleaked"] = merged.apply(
            lambda x: f"{x['Clean_Mean']:.3f} ± {x['Clean_Std']:.3f}", axis=1
        )

        logger.info(merged[["Baseline", "Deleaked"]].to_string())
        merged.to_csv(output_dir / "comparative_feature_stability.csv")
    else:
        logger.info("Feature stability matrix could not be computed.")

    # 3. Leakage Quantification - Jaccard per Class per Top_N
    logger.info("\n3. LEAKAGE QUANTIFICATION (Jaccard Overlap: Baseline vs Deleaked)")
    logger.info(leakage_df.to_string(index=False))
    leakage_df.to_csv(output_dir / "cv_leakage_quantification.csv", index=False)


if __name__ == "__main__":
    main()
