"""
Hyperparameter tuning for Logistic Regression using Optuna.

Optimizes a logistic regression classifier for political leaning prediction
(left, centre, right) with author-aware train/test splitting.

Features:
- Persistent SQLite storage for study results (can restart/resume)
- Author-aware 90/10 train/test split (no author leakage)
- TF-IDF vectorization optimized for large corpora (~55k posts, ~1500 words each)
- Multiclass classification optimized for macro F1 score
"""

import argparse
import json
import logging
from pathlib import Path

import joblib
import optuna
import pandas as pd
from optuna.trial import Trial
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, f1_score
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import FeatureUnion

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LogisticRegressionTuner:
    """
    Tunes Logistic Regression hyperparameters using Optuna.

    The study is persisted to SQLite database, allowing:
    - Resuming interrupted optimization runs
    - Analyzing trial history
    - Sharing results across sessions

    Optimization target: Maximize Macro F1 Score
    """

    def __init__(
        self,
        data_path: Path,
        output_dir: Path,
        study_name: str = "political_leaning_logreg_f1",
        n_trials: int = 100,
        train_size: float = 0.9,
        random_state: int = 42,
        n_jobs: int = 4,
    ):
        """
        Initialize the tuner.

        Args:
            data_path: Path to dataset (CSV or JSON) with columns:
                       'author', 'post', 'political_leaning'
            output_dir: Directory for outputs (db, models, params)
            study_name: Name for the Optuna study (for DB identification)
            n_trials: Number of optimization trials
            train_size: Fraction of authors for training (default 0.9 = 90%)
            random_state: Random seed for reproducibility
            n_jobs: Number of parallel jobs for optimization
        """
        self.data_path = Path(data_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.study_name = study_name
        self.n_trials = n_trials
        self.train_size = train_size
        self.random_state = random_state
        self.n_jobs = n_jobs

        # Data containers
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.vectorizer = None

        # Best model
        self.best_model = None
        self.best_params = None

        # Storage path for Optuna
        self.storage_path = self.output_dir / f"{self.study_name}.db"
        self.storage_url = f"sqlite:///{self.storage_path}"

    def load_and_prepare_data(self) -> None:
        """
        Load data and prepare author-aware train/test splits.

        The split ensures no author appears in both train and test sets,
        preventing author-style leakage in evaluation.
        """
        logger.info(f"Loading data from {self.data_path}")

        # Load data based on file format
        if self.data_path.suffix == ".csv":
            df = pd.read_csv(self.data_path)
        elif self.data_path.suffix == ".json":
            df = pd.read_json(self.data_path, lines=True)
        else:
            raise ValueError(f"Unsupported file format: {self.data_path.suffix}")

        # Validate required columns
        required_cols = ["post", "political_leaning", "author"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Missing required columns: {missing_cols}. "
                f"Dataset must have: {required_cols}"
            )

        # Remove any rows with missing values
        initial_len = len(df)
        df = df.dropna(subset=required_cols)
        if len(df) < initial_len:
            logger.warning(f"Dropped {initial_len - len(df)} rows with missing values")

        logger.info(
            f"Loaded {len(df)} samples, "
            f"{df['political_leaning'].nunique()} classes ({df['political_leaning'].unique().tolist()}), "
            f"{df['author'].nunique()} unique authors"
        )

        # Author-aware split (90/10 by default)
        logger.info(
            f"Performing author-aware split: {self.train_size * 100:.0f}% train / "
            f"{(1 - self.train_size) * 100:.0f}% test"
        )

        splitter = GroupShuffleSplit(
            n_splits=1,
            train_size=self.train_size,
            random_state=self.random_state,
        )

        for train_idx, test_idx in splitter.split(df, groups=df["author"]):
            train_df = df.iloc[train_idx].copy()
            test_df = df.iloc[test_idx].copy()

        # Validate no author overlap
        train_authors = set(train_df["author"].unique())
        test_authors = set(test_df["author"].unique())
        overlap = train_authors & test_authors

        if overlap:
            raise ValueError(f"Author overlap detected between train/test: {overlap}")

        logger.info(
            f"Split complete: "
            f"Train={len(train_df)} samples ({len(train_authors)} authors), "
            f"Test={len(test_df)} samples ({len(test_authors)} authors)"
        )

        # Log class distribution
        train_dist = train_df["political_leaning"].value_counts(normalize=True)
        test_dist = test_df["political_leaning"].value_counts(normalize=True)
        logger.info(f"Train class distribution:\n{train_dist.to_string()}")
        logger.info(f"Test class distribution:\n{test_dist.to_string()}")

        # Feature engineering with TF-IDF
        logger.info("Engineering TF-IDF features...")
        self.vectorizer = FeatureUnion(
            [
                (
                    "word_tfidf",
                    TfidfVectorizer(
                        analyzer="word",
                        ngram_range=(1, 2),  # Token unigrams and bigrams
                        min_df=3,
                        max_df=0.90,
                        sublinear_tf=True,
                        smooth_idf=True,  # IDF smoothing (default is True)
                    ),
                ),
                (
                    "char_tfidf",
                    TfidfVectorizer(
                        analyzer="char",
                        ngram_range=(3, 3),  # Character tri-grams only
                        min_df=3,
                        max_df=0.90,
                        sublinear_tf=True,
                        smooth_idf=True,
                    ),
                ),
            ]
        )

        self.X_train = self.vectorizer.fit_transform(train_df["post"].tolist())
        self.X_test = self.vectorizer.transform(test_df["post"].tolist())
        self.y_train = train_df["political_leaning"].values
        self.y_test = test_df["political_leaning"].values

        logger.info(
            f"Features: Train shape={self.X_train.shape}, "
            f"Test shape={self.X_test.shape}"
        )

    def objective(self, trial: Trial) -> float:
        """
        Optuna objective function to MAXIMIZE Macro F1 Score.

        Searches over:
        - l1_ratio: Mix of L1/L2 regularization (0=L2, 1=L1)
        - C: Inverse regularization strength
        - tol: Tolerance for stopping criterion
        - max_iter: Maximum iterations for solver

        Returns:
            Test set macro F1 score (higher is better)
        """
        # L1 ratio: 0 = pure L2, 1 = pure L1, between = elasticnet mix
        l1_ratio = trial.suggest_float("l1_ratio", 0.0, 1.0)

        # Regularization strength (inverse)
        C = trial.suggest_float("C", 1e-1, 1e2, log=True)

        # Tolerance for stopping criterion
        tol = trial.suggest_float("tol", 1e-5, 1e-2, log=True)

        # Maximum iterations
        max_iter = trial.suggest_int("max_iter", 200, 2000, log=True)

        # Build model
        model = LogisticRegression(
            C=C,
            solver="saga",
            l1_ratio=l1_ratio,
            tol=tol,
            max_iter=max_iter,
            random_state=self.random_state,
        )

        model.fit(self.X_train, self.y_train)

        # Evaluate on test set
        preds = model.predict(self.X_test)
        f1 = f1_score(self.y_test, preds, average="macro")

        return f1

    def run_optimization(self) -> optuna.Study:
        """
        Run Optuna hyperparameter optimization.

        Uses SQLite storage for persistence - can resume if interrupted.

        Returns:
            The completed Optuna study
        """
        logger.info(f"Starting Optuna optimization with {self.n_trials} trials")
        logger.info(f"Study database: {self.storage_path}")
        logger.info("Optimization target: MAXIMIZE Macro F1 Score")

        # TPE sampler with multivariate option
        sampler = optuna.samplers.TPESampler(
            seed=self.random_state,
            multivariate=True,
            n_startup_trials=15,
        )

        # Create or load existing study - MAXIMIZE F1
        study = optuna.create_study(
            direction="maximize",
            study_name=self.study_name,
            sampler=sampler,
            storage=self.storage_url,
            load_if_exists=True,
        )

        # Check if we have existing trials
        completed_trials = len(
            [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
        )
        if completed_trials > 0:
            logger.info(f"Resuming study with {completed_trials} existing trials")
            logger.info(f"Current best F1: {study.best_value:.4f}")

        # Run optimization
        study.optimize(
            self.objective,
            n_trials=self.n_trials,
            show_progress_bar=True,
            n_jobs=self.n_jobs,
        )

        logger.info("Optimization complete!")
        logger.info(f"Best Macro F1: {study.best_value:.4f}")
        logger.info(f"Best parameters: {study.best_params}")

        self.best_params = study.best_params
        return study

    def train_best_model(self) -> LogisticRegression:
        """
        Train final model with best hyperparameters.

        Returns:
            Trained model with best parameters
        """
        logger.info("Training final model with best hyperparameters")

        p = self.best_params

        self.best_model = LogisticRegression(
            C=p["C"],
            solver="saga",
            l1_ratio=p["l1_ratio"],
            tol=p["tol"],
            max_iter=p["max_iter"] * 2,
            random_state=self.random_state,
        )

        self.best_model.fit(self.X_train, self.y_train)

        # Evaluate on test set
        preds = self.best_model.predict(self.X_test)
        test_f1 = f1_score(self.y_test, preds, average="macro")

        logger.info(f"Final Test Macro F1: {test_f1:.4f}")
        logger.info(
            f"Best params: C={p['C']:.4f}, l1_ratio={p['l1_ratio']:.3f}, "
            f"class_weight={p['class_weight']}, tol={p['tol']:.2e}, "
            f"max_iter={p['max_iter']}"
        )
        logger.info(
            f"\nClassification Report:\n{classification_report(self.y_test, preds)}"
        )

        return self.best_model

    def save_results(self, study: optuna.Study) -> None:
        """Save model, vectorizer, and parameters."""
        logger.info(f"Saving results to {self.output_dir}")

        # Save model
        if self.best_model is not None:
            model_path = self.output_dir / "best_model.pkl"
            joblib.dump(self.best_model, model_path)
            logger.info(f"Saved model to {model_path}")

        # Save vectorizer
        vectorizer_path = self.output_dir / "vectorizer.pkl"
        joblib.dump(self.vectorizer, vectorizer_path)
        logger.info(f"Saved vectorizer to {vectorizer_path}")

        # Save best parameters
        params_path = self.output_dir / "best_params.json"
        with open(params_path, "w") as f:
            json.dump(self.best_params, f, indent=2)
        logger.info(f"Saved parameters to {params_path}")

        # Compute test F1 for summary
        test_f1 = None
        if self.best_model is not None:
            preds = self.best_model.predict(self.X_test)
            test_f1 = float(f1_score(self.y_test, preds, average="macro"))

        # Save study summary
        summary = {
            "study_name": self.study_name,
            "optimization_target": "macro_f1",
            "best_f1": study.best_value,
            "best_test_f1": test_f1,
            "best_params": study.best_params,
            "n_trials": len(study.trials),
            "n_completed_trials": len(
                [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
            ),
            "train_size": self.train_size,
            "storage_path": str(self.storage_path),
        }
        summary_path = self.output_dir / "study_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Saved study summary to {summary_path}")

    def run(self) -> optuna.Study:
        """
        Run the complete tuning pipeline.

        Returns:
            The completed Optuna study
        """
        self.load_and_prepare_data()
        study = self.run_optimization()
        self.train_best_model()
        self.save_results(study)
        return study


def main():
    parser = argparse.ArgumentParser(
        description="Tune Logistic Regression for Political Leaning Classification (Macro F1)"
    )
    parser.add_argument(
        "--data",
        type=str,
        default="preprocessed_data/preprocessed_data.csv",
        help="Path to dataset (CSV/JSON) with 'author', 'post', 'political_leaning' columns",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="outputs/logreg_tuning_f1",
        help="Output directory for results",
    )
    parser.add_argument(
        "--study-name",
        type=str,
        default="political_leaning_logreg_f1",
        help="Name for the Optuna study (used in database)",
    )
    parser.add_argument(
        "--n-trials",
        type=int,
        default=100,
        help="Number of optimization trials",
    )
    parser.add_argument(
        "--train-size",
        type=float,
        default=0.9,
        help="Fraction of authors for training (default: 0.9 = 90%%)",
    )
    parser.add_argument(
        "--n-jobs",
        type=int,
        default=6,
        help="Number of parallel jobs for optimization",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )

    args = parser.parse_args()

    tuner = LogisticRegressionTuner(
        data_path=Path(args.data),
        output_dir=Path(args.output),
        study_name=args.study_name,
        n_trials=args.n_trials,
        train_size=args.train_size,
        random_state=args.random_state,
        n_jobs=args.n_jobs,
    )
    tuner.run()


if __name__ == "__main__":
    main()
