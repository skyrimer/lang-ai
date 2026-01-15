"""
Comparison runner script for evaluating deleaking methodology.

Runs evaluation on both baseline and sanitized datasets, generates
side-by-side comparisons, and exports structured results.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional

import hydra
import pandas as pd
from omegaconf import DictConfig

from src.lang_ai.core.logger import setup_logging

from .deleaking_evaluator import DeleakingEvaluator, EvaluationConfig, EvaluationResults
from .metrics import FeatureComparator

logger = setup_logging()


class DeleakingComparison:
    """
    Compares baseline and sanitized corpus evaluations.

    Generates comprehensive comparison reports highlighting:
    - Stylometric shifts
    - Geometric changes
    - Performance trade-offs
    """

    def __init__(
        self, baseline_results: EvaluationResults, sanitized_results: EvaluationResults
    ):
        """
        Initialize comparison with evaluation results.

        Args:
            baseline_results: Results from baseline corpus
            sanitized_results: Results from sanitized corpus
        """
        self.baseline = baseline_results
        self.sanitized = sanitized_results
        self.feature_comparator = FeatureComparator()

    def compare_stylometry(self) -> Dict[str, Any]:
        """
        Compare stylometric properties between corpora.

        Returns:
            Dictionary with stylometric comparison metrics
        """
        baseline_overall = self.baseline.stylometric_analysis["overall"]
        sanitized_overall = self.sanitized.stylometric_analysis["overall"]

        baseline_mean = baseline_overall["mean_stylometric_ratio"]
        sanitized_mean = sanitized_overall["mean_stylometric_ratio"]

        return {
            "baseline_stylometric_ratio": baseline_mean,
            "sanitized_stylometric_ratio": sanitized_mean,
            "stylometric_ratio_change": sanitized_mean - baseline_mean,
            "stylometric_ratio_change_pct": (
                ((sanitized_mean - baseline_mean) / baseline_mean * 100)
                if baseline_mean > 0
                else 0.0
            ),
            "interpretation": (
                "increase"
                if sanitized_mean > baseline_mean
                else "decrease"
                if sanitized_mean < baseline_mean
                else "no change"
            ),
            "per_class_comparison": {
                cls: {
                    "baseline": self.baseline.stylometric_analysis[cls][
                        "stylometric_ratio"
                    ],
                    "sanitized": self.sanitized.stylometric_analysis[cls][
                        "stylometric_ratio"
                    ],
                    "change": (
                        self.sanitized.stylometric_analysis[cls]["stylometric_ratio"]
                        - self.baseline.stylometric_analysis[cls]["stylometric_ratio"]
                    ),
                }
                for cls in self.baseline.stylometric_analysis
                if cls.startswith("class_")
            },
        }

    def compare_geometry(self) -> Dict[str, Any]:
        """
        Compare geometric properties between corpora.

        Returns:
            Dictionary with geometric comparison metrics
        """
        baseline_geo = self.baseline.geometric_metrics
        sanitized_geo = self.sanitized.geometric_metrics

        baseline_centroid = baseline_geo["mean_centroid_distance"]
        sanitized_centroid = sanitized_geo["mean_centroid_distance"]

        baseline_silhouette = baseline_geo["silhouette_score"]
        sanitized_silhouette = sanitized_geo["silhouette_score"]

        return {
            "centroid_distance": {
                "baseline": baseline_centroid,
                "sanitized": sanitized_centroid,
                "change": sanitized_centroid - baseline_centroid,
                "change_pct": (
                    ((sanitized_centroid - baseline_centroid) / baseline_centroid * 100)
                    if baseline_centroid > 0
                    else 0.0
                ),
            },
            "silhouette_score": {
                "baseline": baseline_silhouette,
                "sanitized": sanitized_silhouette,
                "change": sanitized_silhouette - baseline_silhouette,
                "change_pct": (
                    (
                        (sanitized_silhouette - baseline_silhouette)
                        / abs(baseline_silhouette)
                        * 100
                    )
                    if baseline_silhouette != 0
                    else 0.0
                ),
            },
        }

    def compare_performance(self) -> Dict[str, Any]:
        """
        Compare classification performance between corpora.

        Returns:
            Dictionary with performance comparison for each model
        """
        results = {}

        for model_name in self.baseline.model_results:
            baseline_f1 = self.baseline.model_results[model_name]["macro_f1"]
            sanitized_f1 = self.sanitized.model_results[model_name]["macro_f1"]

            results[model_name] = {
                "baseline_macro_f1": baseline_f1,
                "sanitized_macro_f1": sanitized_f1,
                "f1_change": sanitized_f1 - baseline_f1,
                "f1_change_pct": (
                    ((sanitized_f1 - baseline_f1) / baseline_f1 * 100)
                    if baseline_f1 > 0
                    else 0.0
                ),
                "per_class_f1": {
                    cls: {
                        "baseline": self.baseline.model_results[model_name][
                            "per_class_f1"
                        ][cls],
                        "sanitized": self.sanitized.model_results[model_name][
                            "per_class_f1"
                        ][cls],
                        "change": (
                            self.sanitized.model_results[model_name]["per_class_f1"][
                                cls
                            ]
                            - self.baseline.model_results[model_name]["per_class_f1"][
                                cls
                            ]
                        ),
                    }
                    for cls in self.baseline.model_results[model_name]["per_class_f1"]
                },
            }

        return results

    def compare_features(self) -> Dict[str, Any]:
        """
        Compare feature sets between corpora.

        Returns:
            Dictionary with feature overlap metrics
        """
        return self.feature_comparator.compare_feature_sets(
            self.baseline.top_features_per_class,
            self.sanitized.top_features_per_class,
            top_n=50,
        )

    def generate_comparison_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive comparison report.

        Returns:
            Dictionary with all comparison metrics
        """
        logger.info("Generating comparison report")

        report = {
            "metadata": {
                "baseline_corpus": self.baseline.corpus_name,
                "sanitized_corpus": self.sanitized.corpus_name,
                "baseline_timestamp": self.baseline.timestamp,
                "sanitized_timestamp": self.sanitized.timestamp,
            },
            "dataset_comparison": {
                "baseline": {
                    "train_samples": self.baseline.train_samples,
                    "eval_samples": self.baseline.eval_samples,
                    "num_authors": self.baseline.num_authors,
                },
                "sanitized": {
                    "train_samples": self.sanitized.train_samples,
                    "eval_samples": self.sanitized.eval_samples,
                    "num_authors": self.sanitized.num_authors,
                },
            },
            "stylometric_comparison": self.compare_stylometry(),
            "geometric_comparison": self.compare_geometry(),
            "performance_comparison": self.compare_performance(),
            "feature_overlap": self.compare_features(),
        }

        return report

    def generate_summary_table(self) -> pd.DataFrame:
        """
        Generate summary table for quick comparison.

        Returns:
            DataFrame with key metrics side-by-side
        """
        report = self.generate_comparison_report()

        # Extract key metrics
        rows = []

        # Stylometry
        styl = report["stylometric_comparison"]
        rows.append(
            {
                "Metric": "Stylometric Ratio",
                "Baseline": f"{styl['baseline_stylometric_ratio']:.3f}",
                "Sanitized": f"{styl['sanitized_stylometric_ratio']:.3f}",
                "Change": f"{styl['stylometric_ratio_change']:+.3f}",
                "Change %": f"{styl['stylometric_ratio_change_pct']:+.1f}%",
            }
        )

        # Geometry
        geo = report["geometric_comparison"]
        rows.append(
            {
                "Metric": "Mean Centroid Distance",
                "Baseline": f"{geo['centroid_distance']['baseline']:.3f}",
                "Sanitized": f"{geo['centroid_distance']['sanitized']:.3f}",
                "Change": f"{geo['centroid_distance']['change']:+.3f}",
                "Change %": f"{geo['centroid_distance']['change_pct']:+.1f}%",
            }
        )

        rows.append(
            {
                "Metric": "Silhouette Score",
                "Baseline": f"{geo['silhouette_score']['baseline']:.3f}",
                "Sanitized": f"{geo['silhouette_score']['sanitized']:.3f}",
                "Change": f"{geo['silhouette_score']['change']:+.3f}",
                "Change %": f"{geo['silhouette_score']['change_pct']:+.1f}%",
            }
        )

        # Performance (best model)
        perf = report["performance_comparison"]
        for model_name in ["LogReg_L2", "LogReg_NoReg", "SVM"]:
            if model_name in perf:
                model_perf = perf[model_name]
                rows.append(
                    {
                        "Metric": f"{model_name} Macro F1",
                        "Baseline": f"{model_perf['baseline_macro_f1']:.3f}",
                        "Sanitized": f"{model_perf['sanitized_macro_f1']:.3f}",
                        "Change": f"{model_perf['f1_change']:+.3f}",
                        "Change %": f"{model_perf['f1_change_pct']:+.1f}%",
                    }
                )

        # Feature overlap
        feat = report["feature_overlap"]
        rows.append(
            {
                "Metric": "Feature Overlap (Jaccard)",
                "Baseline": "-",
                "Sanitized": "-",
                "Change": f"{feat['overall']['mean_jaccard']:.3f}",
                "Change %": "-",
            }
        )

        return pd.DataFrame(rows)


def run_comparison(
    baseline_path: Path,
    sanitized_path: Path,
    output_dir: Path,
    config: Optional[EvaluationConfig] = None,
):
    """
    Run complete comparison pipeline.

    Args:
        baseline_path: Path to baseline corpus
        sanitized_path: Path to sanitized corpus
        output_dir: Directory for output files
        config: Evaluation configuration
    """
    logger.info("Starting deleaking comparison pipeline")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Run evaluations
    evaluator_baseline = DeleakingEvaluator(config)
    baseline_results = evaluator_baseline.run_evaluation(baseline_path, "baseline")

    evaluator_sanitized = DeleakingEvaluator(config)
    sanitized_results = evaluator_sanitized.run_evaluation(sanitized_path, "sanitized")

    # Save individual results
    baseline_results.save_json(output_dir / "baseline_evaluation.json")
    sanitized_results.save_json(output_dir / "sanitized_evaluation.json")

    # Save top features
    feature_dir = output_dir / "feature_analysis"
    feature_dir.mkdir(exist_ok=True)

    with open(feature_dir / "baseline_top_features.json", "w") as f:
        json.dump(baseline_results.top_features_per_class, f, indent=2)

    with open(feature_dir / "sanitized_top_features.json", "w") as f:
        json.dump(sanitized_results.top_features_per_class, f, indent=2)

    # Generate comparison
    comparison = DeleakingComparison(baseline_results, sanitized_results)
    comparison_report = comparison.generate_comparison_report()

    # Save comparison report
    with open(output_dir / "comparison_report.json", "w") as f:
        json.dump(comparison_report, f, indent=2)

    # Generate and save summary table
    summary_table = comparison.generate_summary_table()
    summary_table.to_csv(output_dir / "comparison_summary.csv", index=False)

    logger.info(f"\n{summary_table.to_string(index=False)}")
    logger.info(f"\nAll results saved to {output_dir}")


@hydra.main(
    config_path="../../configs", config_name="evaluation_config", version_base=None
)
def main(cfg: DictConfig) -> None:
    """
    Hydra entry point for deleaking comparison.

    Reads baseline and sanitized dataset paths from config.
    """
    baseline_path = Path(cfg.paths.baseline)
    sanitized_path = Path(cfg.paths.sanitized)
    output_dir = Path(cfg.paths.output_dir)

    logger.info("Running deleaking comparison from config")
    logger.info(f"Baseline: {baseline_path}")
    logger.info(f"Sanitized: {sanitized_path}")
    logger.info(f"Output directory: {output_dir}")

    # Run comparison
    run_comparison(
        baseline_path=baseline_path,
        sanitized_path=sanitized_path,
        output_dir=output_dir,
        config=None,  # Uses config from Hydra
    )


if __name__ == "__main__":
    main()
