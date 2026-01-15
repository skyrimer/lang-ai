"""
Core evaluator for deleaking methodology assessment.

Provides author-aware data splitting, feature engineering, model training,
and comprehensive evaluation metrics.
"""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, f1_score
from sklearn.model_selection import GroupShuffleSplit
from sklearn.multiclass import OneVsRestClassifier
from sklearn.svm import LinearSVC

from .metrics import GeometricAnalyzer, StylometricAnalyzer
from src.lang_ai.core.logger import setup_logging

logger = setup_logging()


@dataclass
class EvaluationConfig:
    """Configuration for evaluation pipeline."""

    # TF-IDF parameters
    ngram_range: Tuple[int, int] = (1, 2)
    max_features: int = 10000
    min_df: int = 5
    sublinear_tf: bool = True

    # Train/eval split
    train_size: float = 0.8
    random_state: int = 42

    # Feature analysis
    top_n_features: int = 50

    # Silhouette sampling
    silhouette_sample_size: int = 5000

    # Model configurations
    models: Dict[str, Dict[str, Any]] = None

    def __post_init__(self):
        if self.models is None:
            self.models = {
                "LogReg_NoReg": {
                    "class": LogisticRegression,
                    "params": {
                        "C": 1e10,  # Effectively no regularization
                        "max_iter": 1000,
                        "random_state": self.random_state,
                    },
                },
                "LogReg_L2": {
                    "class": LogisticRegression,
                    "params": {
                        "penalty": "l2",
                        "C": 1.0,
                        "max_iter": 1000,
                        "random_state": self.random_state,
                    },
                },
                "SVM": {
                    "class": LinearSVC,
                    "params": {
                        "loss": "squared_hinge",
                        "max_iter": 1000,
                        "random_state": self.random_state,
                    },
                },
            }


@dataclass
class EvaluationResults:
    """Container for evaluation results."""

    corpus_name: str
    timestamp: str

    # Dataset statistics
    train_samples: int
    eval_samples: int
    num_classes: int
    num_authors: int
    train_authors: int
    eval_authors: int

    # Geometric metrics
    geometric_metrics: Dict[str, Any]

    # Stylometric analysis
    stylometric_analysis: Dict[str, Any]

    # Model performance
    model_results: Dict[str, Dict[str, Any]]

    # Feature information
    top_features_per_class: Dict[str, List[str]]

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return asdict(self)

    def save_json(self, output_path: Path):
        """Save results as JSON."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Results saved to {output_path}")


class DeleakingEvaluator:
    """
    Main evaluator class for assessing deleaking methodology effectiveness.

    Compares baseline (leaky) vs sanitized (clean) corpora by analyzing:
    - Geometric properties (centroid distances, silhouette scores)
    - Stylometric vs topic reliance
    - Classification performance
    """

    def __init__(self, config: Optional[EvaluationConfig] = None):
        """
        Initialize evaluator with configuration.

        Args:
            config: Evaluation configuration (uses defaults if None)
        """
        self.config = config or EvaluationConfig()
        self.geometric_analyzer = GeometricAnalyzer()
        self.stylometric_analyzer = StylometricAnalyzer()
        self.vectorizer = None
        self.models = {}
        self.feature_names = None

    def load_data(
        self,
        data_path: Path,
        text_column: str = "post",
        label_column: str = "political_leaning",
        author_column: str = "author",
    ) -> pd.DataFrame:
        """
        Load dataset from file.

        Args:
            data_path: Path to CSV/JSON file
            text_column: Name of text column
            label_column: Name of label column
            author_column: Name of author column

        Returns:
            DataFrame with required columns
        """
        logger.info(f"Loading data from {data_path}")

        if data_path.suffix == ".csv":
            df = pd.read_csv(data_path)
        elif data_path.suffix == ".json":
            df = pd.read_json(data_path, lines=True)
        else:
            raise ValueError(f"Unsupported file format: {data_path.suffix}")

        # Validate required columns
        required_cols = [text_column, label_column, author_column]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Rename to standard names
        df = df.rename(
            columns={
                text_column: "text",
                label_column: "label",
                author_column: "author",
            }
        )

        logger.info(
            f"Loaded {len(df)} samples, "
            f"{df['label'].nunique()} classes, "
            f"{df['author'].nunique()} authors"
        )

        return df[["text", "label", "author"]]

    def author_aware_split(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Perform author-aware stratified splitting.

        Ensures no author appears in both train and eval sets.

        Args:
            df: DataFrame with 'text', 'label', 'author' columns

        Returns:
            Tuple of (train_df, eval_df)
        """
        logger.info("Performing author-aware split")

        splitter = GroupShuffleSplit(
            n_splits=1,
            train_size=self.config.train_size,
            random_state=self.config.random_state,
        )

        # Split on author level
        for train_idx, eval_idx in splitter.split(df, groups=df["author"]):
            train_df = df.iloc[train_idx].copy()
            eval_df = df.iloc[eval_idx].copy()

        # Validate no author overlap
        train_authors = set(train_df["author"].unique())
        eval_authors = set(eval_df["author"].unique())
        overlap = train_authors & eval_authors

        if overlap:
            raise ValueError(f"Author overlap detected: {overlap}")

        logger.info(
            f"Split complete: "
            f"Train={len(train_df)} samples ({len(train_authors)} authors), "
            f"Eval={len(eval_df)} samples ({len(eval_authors)} authors)"
        )

        # Check class distribution
        train_dist = train_df["label"].value_counts(normalize=True)
        eval_dist = eval_df["label"].value_counts(normalize=True)
        logger.info(f"Train class distribution:\n{train_dist}")
        logger.info(f"Eval class distribution:\n{eval_dist}")

        return train_df, eval_df

    def engineer_features(
        self, train_texts: List[str], eval_texts: List[str]
    ) -> Tuple[csr_matrix, csr_matrix]:
        """
        Engineer TF-IDF features with stylometric preservation.

        Args:
            train_texts: Training text samples
            eval_texts: Evaluation text samples

        Returns:
            Tuple of (train_features, eval_features) as sparse matrices
        """
        logger.info("Engineering TF-IDF features")

        self.vectorizer = TfidfVectorizer(
            ngram_range=self.config.ngram_range,
            max_features=self.config.max_features,
            min_df=self.config.min_df,
            sublinear_tf=self.config.sublinear_tf,
            stop_words=None,  # Critical: no stop word removal for stylometry
            lowercase=True,
        )

        X_train = self.vectorizer.fit_transform(train_texts)
        X_eval = self.vectorizer.transform(eval_texts)

        self.feature_names = np.array(self.vectorizer.get_feature_names_out())

        logger.info(
            f"Features engineered: "
            f"Train shape={X_train.shape}, Eval shape={X_eval.shape}"
        )

        return X_train, X_eval

    def train_models(
        self, X_train: csr_matrix, y_train: np.ndarray
    ) -> Dict[str, OneVsRestClassifier]:
        """
        Train all models with One-vs-Rest multiclass strategy.

        Args:
            X_train: Training features
            y_train: Training labels

        Returns:
            Dictionary of trained models
        """
        logger.info("Training models")

        for model_name, model_config in self.config.models.items():
            logger.info(f"Training {model_name}")

            base_classifier = model_config["class"](**model_config["params"])
            classifier = OneVsRestClassifier(base_classifier)
            classifier.fit(X_train, y_train)

            self.models[model_name] = classifier

        logger.info(f"Trained {len(self.models)} models")
        return self.models

    def evaluate_models(
        self, X_eval: csr_matrix, y_eval: np.ndarray
    ) -> Dict[str, Dict[str, Any]]:
        """
        Evaluate all trained models.

        Args:
            X_eval: Evaluation features
            y_eval: True labels

        Returns:
            Dictionary of model performance metrics
        """
        logger.info("Evaluating models")

        results = {}

        for model_name, model in self.models.items():
            y_pred = model.predict(X_eval)

            # Compute metrics
            macro_f1 = f1_score(y_eval, y_pred, average="macro")
            per_class_f1 = f1_score(y_eval, y_pred, average=None)
            conf_matrix = confusion_matrix(y_eval, y_pred)

            # Classification report
            report = classification_report(y_eval, y_pred, output_dict=True)

            results[model_name] = {
                "macro_f1": float(macro_f1),
                "per_class_f1": {
                    f"class_{cls}": float(score)
                    for cls, score in zip(np.unique(y_eval), per_class_f1)
                },
                "confusion_matrix": conf_matrix.tolist(),
                "classification_report": report,
            }

            logger.info(f"{model_name} - Macro F1: {macro_f1:.4f}")

        return results

    def extract_top_features(
        self, X_train: csr_matrix, y_train: np.ndarray
    ) -> Dict[str, List[str]]:
        """
        Extract top-N features per class based on mean TF-IDF scores.

        Args:
            X_train: Training features
            y_train: Training labels

        Returns:
            Dictionary mapping class -> list of top features
        """
        logger.info("Extracting top features per class")

        top_features = {}

        for cls in np.unique(y_train):
            # Get samples for this class
            mask = y_train == cls
            class_features = X_train[mask]

            # Compute mean TF-IDF per feature
            mean_tfidf = np.asarray(class_features.mean(axis=0)).flatten()

            # Get top-N indices
            top_indices = np.argsort(mean_tfidf)[::-1][: self.config.top_n_features]

            # Map to feature names
            top_features[str(cls)] = self.feature_names[top_indices].tolist()

        return top_features

    def analyze_stylometry(
        self, top_features_per_class: Dict[str, List[str]]
    ) -> Dict[str, Any]:
        """
        Analyze stylometric properties of top features.

        Args:
            top_features_per_class: Top features for each class

        Returns:
            Stylometric analysis results
        """
        logger.info("Analyzing stylometric properties")

        results = {}

        for cls, features in top_features_per_class.items():
            analysis = self.stylometric_analyzer.compute_stylometric_ratio(
                features, top_n=self.config.top_n_features
            )
            results[f"class_{cls}"] = analysis

        # Compute overall averages
        all_ratios = [res["stylometric_ratio"] for res in results.values()]
        results["overall"] = {
            "mean_stylometric_ratio": float(np.mean(all_ratios)),
            "std_stylometric_ratio": float(np.std(all_ratios)),
            "min_stylometric_ratio": float(np.min(all_ratios)),
            "max_stylometric_ratio": float(np.max(all_ratios)),
        }

        return results

    def run_evaluation(self, data_path: Path, corpus_name: str) -> EvaluationResults:
        """
        Run complete evaluation pipeline.

        Args:
            data_path: Path to dataset
            corpus_name: Name identifier (e.g., 'baseline', 'sanitized')

        Returns:
            EvaluationResults object
        """
        logger.info(f"Starting evaluation for corpus: {corpus_name}")

        # Load data
        df = self.load_data(data_path)

        # Author-aware split
        train_df, eval_df = self.author_aware_split(df)

        # Engineer features
        X_train, X_eval = self.engineer_features(
            train_df["text"].tolist(), eval_df["text"].tolist()
        )

        y_train = train_df["label"].values
        y_eval = eval_df["label"].values

        # Geometric analysis
        logger.info("Computing geometric metrics")
        centroid_distances = self.geometric_analyzer.compute_centroid_distances(
            X_train, y_train
        )
        silhouette = self.geometric_analyzer.compute_silhouette(
            X_train, y_train, sample_size=self.config.silhouette_sample_size
        )

        geometric_metrics = {**centroid_distances, "silhouette_score": silhouette}

        # Train models
        self.train_models(X_train, y_train)

        # Evaluate models
        model_results = self.evaluate_models(X_eval, y_eval)

        # Feature analysis
        top_features = self.extract_top_features(X_train, y_train)
        stylometric_analysis = self.analyze_stylometry(top_features)

        # Compile results
        results = EvaluationResults(
            corpus_name=corpus_name,
            timestamp=datetime.now().isoformat(),
            train_samples=len(train_df),
            eval_samples=len(eval_df),
            num_classes=len(np.unique(y_train)),
            num_authors=len(df["author"].unique()),
            train_authors=len(train_df["author"].unique()),
            eval_authors=len(eval_df["author"].unique()),
            geometric_metrics=geometric_metrics,
            stylometric_analysis=stylometric_analysis,
            model_results=model_results,
            top_features_per_class=top_features,
        )

        logger.info(f"Evaluation complete for {corpus_name}")
        return results
