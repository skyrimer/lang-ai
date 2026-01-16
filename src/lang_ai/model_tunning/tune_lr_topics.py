"""
Hyperparameter tuning for Logistic Regression using Optuna with Cross-Validation.

Optimizes a Logistic Regression classifier for political leaning prediction
(left, centre, right) using Stratified Group K-Fold Cross Validation.

Features:
- Persistent SQLite storage for study results
- Stratified Group K-Fold (5 splits): Ensures authors don't leak between folds
- Single TF-IDF vectorization (computed on entire corpus)
- Stability Metrics: Tracks Mean/Std F1 and Feature Jaccard Overlap across folds
"""

import argparse
import json
import logging
import numpy as np
import joblib
import optuna
import pandas as pd
from pathlib import Path
from itertools import combinations
from typing import List, Set, Dict

from optuna.trial import Trial
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.linear_model import LogisticRegression

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LogisticRegressionTuner:
    """
    Tunes Logistic Regression hyperparameters using Optuna with Cross-Validation.
    """

    def __init__(
        self,
        data_path: Path,
        output_dir: Path,
        study_name: str = "political_leaning_logreg_cv",
        n_trials: int = 100,
        n_folds: int = 5,
        random_state: int = 42,
        n_jobs: int = 4,
    ):
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.study_name = study_name
        self.n_trials = n_trials
        self.n_folds = n_folds
        self.random_state = random_state
        self.n_jobs = n_jobs

        # Data containers
        self.X = None
        self.y = None
        self.groups = None  # Author IDs for group splitting
        self.vectorizer = None

        # Best model artifacts
        self.best_model = None
        self.best_params = None

        self.storage_path = self.output_dir / f"{self.study_name}.db"
        self.storage_url = f"sqlite:///{self.storage_path}"

    def load_and_prepare_data(self) -> None:
        """
        Load full dataset and compute TF-IDF on the entire corpus.
        """
        logger.info(f"Loading data from {self.data_path}")

        if self.data_path.suffix == ".csv":
            df = pd.read_csv(self.data_path)
        elif self.data_path.suffix == ".json":
            df = pd.read_json(self.data_path, lines=True)
        else:
            raise ValueError(f"Unsupported file format: {self.data_path.suffix}")

        required_cols = ["post", "political_leaning", "author"]
        df = df.dropna(subset=required_cols)

        logger.info(
            f"Loaded {len(df)} samples, "
            f"{df['political_leaning'].nunique()} classes, "
            f"{df['author'].nunique()} unique authors"
        )

        # 1. Store groups and labels for CV
        self.groups = df["author"].values
        self.y = df["political_leaning"].values

        # 2. Vectorize Entire Corpus (Reuse for all folds)
        logger.info("Engineering TF-IDF features on full corpus...")
        self.vectorizer = TfidfVectorizer(
            analyzer="word",
            ngram_range=(1, 3),
            min_df=5,
            max_df=0.90,
            max_features=20_000,
            sublinear_tf=True,
            stop_words="english",
        )

        self.X = self.vectorizer.fit_transform(df["post"].tolist())
        logger.info(f"Feature matrix shape: {self.X.shape}")

    def calculate_jaccard_stability(self, fold_top_features: List[Dict[int, Set[int]]]) -> float:
        """
        Computes the mean Jaccard overlap of top features between all pairs of folds.

        Args:
            fold_top_features: List where each element is a dict mapping Class ID -> Set of Feature Indices
                               Example: [{0: {1, 5, 9}, 1: {2, 8}}, {0: {1, 5, 10}, ...}]
        Returns:
            Average Jaccard similarity across all class-fold combinations.
        """
        if len(fold_top_features) < 2:
            return 0.0

        jaccard_scores = []
        classes = fold_top_features[0].keys()

        # For each class, compare every pair of folds
        for cls_idx in classes:
            fold_sets = [f[cls_idx] for f in fold_top_features]

            for set_a, set_b in combinations(fold_sets, 2):
                intersection = len(set_a.intersection(set_b))
                union = len(set_a.union(set_b))
                score = intersection / union if union > 0 else 0.0
                jaccard_scores.append(score)

        return float(np.mean(jaccard_scores))

    def objective(self, trial: Trial) -> float:
        """
        Performs Stratified Group K-Fold CV using Logistic Regression.
        Tracks F1 Mean, F1 Std, and Feature Stability.
        """
        # --- Hyperparameters for Logistic Regression ---
        C = trial.suggest_float("C", 1e-3, 1e2, log=True)
        l1_ratio = trial.suggest_float("l1_ratio", 0.0, 1.0)
        tol = trial.suggest_float("tol", 1e-5, 1e-2, log=True)
        max_iter = trial.suggest_int("max_iter", 100, 1000, log=True)

        # We use 'saga' solver because it supports elasticnet (L1 + L2 mixing)
        # and is efficient for large datasets.
        params = {
            "C": C,
            "l1_ratio": l1_ratio,
            "tol": tol,
            "max_iter": max_iter,
            "solver": "saga",
            "random_state": self.random_state,
            "class_weight": "balanced",
        }

        # --- Cross Validation ---
        cv = StratifiedGroupKFold(n_splits=self.n_folds)

        f1_scores = []
        fold_top_features = []  # To store top feature indices per fold

        # Loop over folds
        for fold_idx, (train_idx, val_idx) in enumerate(cv.split(self.X, self.y, groups=self.groups)):
            X_train_fold, X_val_fold = self.X[train_idx], self.X[val_idx]
            y_train_fold, y_val_fold = self.y[train_idx], self.y[val_idx]

            model = LogisticRegression(**params)
            model.fit(X_train_fold, y_train_fold)

            # 1. Performance Metric
            preds = model.predict(X_val_fold)
            score = f1_score(y_val_fold, preds, average="macro")
            f1_scores.append(score)

            # 2. Feature Extraction (Top 50 indices per class)
            # LogisticRegression coef_ shape is (1, n_features) for binary
            # or (n_classes, n_features) for multinomial/OvR.
            top_feats_indices = {}
            if model.coef_.ndim == 1:
                # Binary case: Class 1 is positive (high coef), Class 0 is negative (low coef)
                top_feats_indices[1] = set(np.argsort(model.coef_)[-50:])  # Most positive
                top_feats_indices[0] = set(np.argsort(model.coef_)[:50])   # Most negative
            else:
                # Multiclass case
                for cls_idx in range(model.coef_.shape[0]):
                    # Get indices of top 50 largest coefficients
                    top_feats_indices[cls_idx] = set(np.argsort(model.coef_[cls_idx])[-50:])

            fold_top_features.append(top_feats_indices)

        # --- Aggregation & Reporting ---
        mean_f1 = np.mean(f1_scores)
        std_f1 = np.std(f1_scores)

        # Calculate Stability (Jaccard)
        jaccard_stability = self.calculate_jaccard_stability(fold_top_features)

        # Log these extra stats to Optuna for analysis
        trial.set_user_attr("std_f1", std_f1)
        trial.set_user_attr("jaccard_stability", jaccard_stability)
        trial.set_user_attr("mean_f1", mean_f1)

        return mean_f1

    def run_optimization(self) -> optuna.Study:
        logger.info(f"Starting CV Optimization ({self.n_folds} folds, {self.n_trials} trials)")
        logger.info(f"Study DB: {self.storage_path}")

        sampler = optuna.samplers.TPESampler(seed=self.random_state, multivariate=True)

        study = optuna.create_study(
            direction="maximize",
            study_name=self.study_name,
            sampler=sampler,
            storage=self.storage_url,
            load_if_exists=True,
        )

        study.optimize(self.objective, n_trials=self.n_trials, show_progress_bar=True, n_jobs=self.n_jobs)

        logger.info("Optimization Complete.")
        logger.info(f"Best Mean F1: {study.best_value:.4f}")

        # Retrieve stability stats of the best trial
        best_trial = study.best_trial
        logger.info(f"Best Params: {best_trial.params}")
        logger.info(f"F1 Std Dev: {best_trial.user_attrs.get('std_f1', 0):.4f}")
        logger.info(f"Feature Stability (Jaccard): {best_trial.user_attrs.get('jaccard_stability', 0):.4f}")

        self.best_params = study.best_params
        return study

    def train_final_model(self) -> None:
        """
        Train the final model on the ENTIRE dataset using best parameters.
        """
        logger.info("Training final model on full dataset...")

        # Add fixed params not in search space
        final_params = self.best_params.copy()
        final_params["random_state"] = self.random_state
        final_params["class_weight"] = "balanced"
        final_params["solver"] = "saga"
        final_params["penalty"] = "elasticnet"
        # Increase max_iter for final build to ensure convergence on full data
        final_params["max_iter"] = final_params["max_iter"] * 2

        self.best_model = LogisticRegression(**final_params)
        self.best_model.fit(self.X, self.y)

        logger.info("Final model training complete.")

    def save_results(self, study: optuna.Study) -> None:
        logger.info(f"Saving artifacts to {self.output_dir}")

        if self.best_model:
            joblib.dump(self.best_model, self.output_dir / "best_model.pkl")

        joblib.dump(self.vectorizer, self.output_dir / "vectorizer.pkl")

        with open(self.output_dir / "best_params.json", "w") as f:
            json.dump(self.best_params, f, indent=2)

        # Save detailed summary including stability metrics
        summary = {
            "study_name": self.study_name,
            "best_mean_f1": study.best_value,
            "best_params": study.best_params,
            "stability_metrics": {
                "std_f1": study.best_trial.user_attrs.get("std_f1"),
                "jaccard_stability": study.best_trial.user_attrs.get("jaccard_stability")
            },
            "n_trials": len(study.trials),
            "n_folds": self.n_folds
        }

        with open(self.output_dir / "study_summary.json", "w") as f:
            json.dump(summary, f, indent=2)

        # Export full dataframe for deep analysis of stability vs performance trade-offs
        df_trials = study.trials_dataframe()
        df_trials.to_csv(self.output_dir / "trials_history.csv", index=False)
        logger.info(f"Saved trials history to {self.output_dir / 'trials_history.csv'}")

    def run(self) -> None:
        self.load_and_prepare_data()
        study = self.run_optimization()
        self.train_final_model()
        self.save_results(study)


def main():
    parser = argparse.ArgumentParser(description="Tune Logistic Regression with CV and Stability Analysis")
    parser.add_argument("--data", type=str, default="preprocessed_data/preprocessed_data.csv")
    parser.add_argument("--output", type=str, default="outputs/logreg_cv_stability")
    parser.add_argument("--study-name", type=str, default="logreg_cv_stability")
    parser.add_argument("--n-trials", type=int, default=100)
    parser.add_argument("--n-folds", type=int, default=5)
    parser.add_argument("--n-jobs", type=int, default=6)

    args = parser.parse_args()

    tuner = LogisticRegressionTuner(
        data_path=Path(args.data),
        output_dir=Path(args.output),
        study_name=args.study_name,
        n_trials=args.n_trials,
        n_folds=args.n_folds,
        n_jobs=args.n_jobs,
    )
    tuner.run()


if __name__ == "__main__":
    main()