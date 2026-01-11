"""
Analysis tools for evaluating and comparing LLM Judge results.
Includes Dawid-Skene truth estimation, Krippendorff's Alpha, Brier scores, and bootstrapping.
"""

import ast
import glob
import os
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from rich.console import Console
from rich.table import Table
from tqdm.auto import tqdm

from src.lang_ai.core.logger import setup_logging
from src.lang_ai.core.models import (
    Classification,
    Forensics,
    JudgeEvaluation,
    JudgeResult,
    Judgment,
)

logger = setup_logging(__name__)

# Constants for Mapping
CONFIDENCE_MAP = {"HIGH": 0.95, "MEDIUM": 0.75, "LOW": 0.55}
SEVERITY_WEIGHTS = {"CRITICAL": 5, "MODERATE": 3, "LOW": 1}


class JudgeEvaluator:
    def __init__(
        self,
        judges: List[JudgeEvaluation],
        golden_labels: Dict[str, str] | None = None,
    ):
        self.judges = judges
        self.num_judges = len(judges)
        self.num_posts = len(judges[0].posts) if judges else 0
        self.golden_labels = golden_labels or {}

    @staticmethod
    def _normalize_text(text: list[str]) -> list[str]:
        return [t.strip().lower() for t in text]

    def _verify_spans(self, result: JudgeResult, post: str) -> bool:
        """
        Verifies if the evidence spans cited by the judge actually exist in the text.
        """
        if not result.judgment.is_leaky:
            return True

        # If leaky but no forensics/spans, it's a hallucination
        if not result.forensics or not result.forensics.evidence_spans:
            return False

        original_text_normed = post.lower()
        normed_evidence_spans = self._normalize_text(result.forensics.evidence_spans)

        if not normed_evidence_spans:
            return False

        for span in normed_evidence_spans:
            if span not in original_text_normed:
                return False
        return True

    def calculate_dawid_skene_truth(
        self, max_iter: int = 20, tol: float = 1e-4
    ) -> Tuple[List[bool], Dict[str, float]]:
        """
        Implements Expectation-Maximization (Dawid-Skene) to estimate:
        1. The Latent Truth (Silver Standard)
        2. The algorithmic reliability of each judge.
        """
        if not self.judges:
            return [], {}

        # Initialize Votes Matrix (Judges x Posts)
        # 0 = Safe, 1 = Leaky, -1 = Invalid/Missing
        votes = np.full((self.num_judges, self.num_posts), -1)
        judge_ids = [j.judge_id for j in self.judges]

        for j_idx, judge in enumerate(self.judges):
            for p_idx, result in enumerate(judge.judge_results):
                post = judge.posts[p_idx]
                if self._verify_spans(result, post):
                    votes[j_idx, p_idx] = 1 if result.judgment.is_leaky else 0
                else:
                    # Treat hallucinated spans as missing data for Truth calculation
                    # (We penalize the judge for this later in ranking, but don't let it skew truth)
                    votes[j_idx, p_idx] = -1

        # Initialize priors (Simple Majority)
        valid_votes_mask = votes != -1
        if not np.any(valid_votes_mask):
            return [False] * self.num_posts, {jid: 0.0 for jid in judge_ids}

        counts = np.sum(valid_votes_mask, axis=0)
        sums = np.sum(np.where(valid_votes_mask, votes, 0), axis=0)
        # Initial prob that a post is Leaky (Class 1)
        class_marginals = np.divide(
            sums, counts, out=np.zeros_like(sums, dtype=float), where=counts != 0
        )

        # Error rates: [Judge, True_Class(0,1), Pred_Class(0,1)]
        error_rates = np.zeros((self.num_judges, 2, 2))

        # EM Loop
        for _ in range(max_iter):
            old_class_marginals = class_marginals.copy()

            # --- M-STEP: Estimate Judge Error Rates ---
            for j in range(self.num_judges):
                for t in [0, 1]:  # True Class
                    for c in [0, 1]:  # Predicted Class
                        # Weighted count of times judge predicted c when truth was likely t
                        weights = class_marginals if t == 1 else (1 - class_marginals)
                        match_mask = (votes[j] == c) & valid_votes_mask[j]

                        numerator = np.sum(weights[match_mask])
                        denominator = np.sum(weights[valid_votes_mask[j]])

                        error_rates[j, t, c] = (
                            (numerator / denominator) if denominator > 0 else 0.5
                        )

            # --- E-STEP: Update Truth Estimates ---
            new_marginals = np.zeros(self.num_posts)
            p_class_1 = np.mean(class_marginals)  # Prior for class 1
            p_class_0 = 1 - p_class_1

            for i in range(self.num_posts):
                l1, l0 = p_class_1, p_class_0
                for j in range(self.num_judges):
                    if not valid_votes_mask[j, i]:
                        continue
                    vote = votes[j, i]
                    l1 *= error_rates[j, 1, vote] + 1e-9
                    l0 *= error_rates[j, 0, vote] + 1e-9

                new_marginals[i] = l1 / (l1 + l0) if (l1 + l0) > 0 else 0.5

            class_marginals = new_marginals
            if np.max(np.abs(class_marginals - old_class_marginals)) < tol:
                break

        # Finalize
        silver_labels = [p > 0.5 for p in class_marginals]

        # Calculate algorithmic reliability (Diagonal of confusion matrix)
        reliability = {}
        for j, jid in enumerate(judge_ids):
            # Avg of Specificity (True Negative Rate) and Sensitivity (True Positive Rate)
            score = (error_rates[j, 0, 0] + error_rates[j, 1, 1]) / 2
            reliability[jid] = round(score, 3)

        return silver_labels, reliability

    @staticmethod
    def _calculate_brier_score(confidence_lvl: str, is_correct: bool) -> float:
        """
        Calculates individual Brier score component.
        Lower is better. 0 = Perfect, 1 = Worst.
        """
        prob = CONFIDENCE_MAP.get(confidence_lvl, 0.55)
        # If judge is correct, target is 1.0. If wrong, target is 0.0.
        target = 1.0 if is_correct else 0.0
        return (prob - target) ** 2

    def rank_judges(self) -> pd.DataFrame:
        silver_labels, reliability_scores = self.calculate_dawid_skene_truth()

        # If golden labels are provided, they take priority for the 'Final Truth'
        final_truth = []
        for i in range(self.num_posts):
            post = self.judges[0].posts[i]
            if post in self.golden_labels:
                final_truth.append(self.golden_labels[post] == "LEAKY")
            else:
                final_truth.append(silver_labels[i])

        judge_scores = self._score_judges(final_truth, reliability_scores)

        df = pd.DataFrame(judge_scores).T
        # Normalize columns for better reading
        cols = [
            "DS Reliability",
            "Brier Score (Calib)",
            "Hallucinations",
        ]

        # Add Golden metrics if available
        golden_cols = ["Golden Recall", "Golden F1"]
        if any(col in df.columns for col in golden_cols):
            # Only include columns that were actually calculated
            available_golden = [col for col in golden_cols if col in df.columns]
            cols.extend(available_golden)

        return df[cols].sort_values("DS Reliability", ascending=False)

    def _score_judges(
        self, silver_labels: List[bool], reliability_scores: Dict[str, float]
    ) -> Dict[str, Dict[str, Any]]:
        judge_scores = {}
        if not self.judges:
            return {}

        for judge in self.judges:
            metrics = {
                "agreements": 0,
                "hallucinations": 0,
                "brier_sum": 0.0,
                # Golden set metrics
                "golden_tp": 0,
                "golden_fp": 0,
                "golden_tn": 0,
                "golden_fn": 0,
            }

            for i in range(self.num_posts):
                result = judge.judge_results[i]
                post = judge.posts[i]
                silver_is_leaky = silver_labels[i]

                # 1. Hallucination Check
                if not self._verify_spans(result, post):
                    metrics["hallucinations"] += 1
                    # Treat hallucination as a generic Wrong Answer for other metrics
                    is_correct = False
                    is_leaky_vote = result.judgment.is_leaky  # Still what they voted
                else:
                    is_leaky_vote = result.judgment.is_leaky
                    is_correct = is_leaky_vote == silver_is_leaky

                # Brier Score (Calibration)
                # If hallucination, we assume they were confident but wrong.
                metrics["brier_sum"] += self._calculate_brier_score(
                    result.judgment.confidence, is_correct
                )

                # Golden Set Metrics
                if post in self.golden_labels:
                    golden_is_leaky = self.golden_labels[post] == "LEAKY"
                    if is_leaky_vote and golden_is_leaky:
                        metrics["golden_tp"] += 1
                    elif is_leaky_vote and not golden_is_leaky:
                        metrics["golden_fp"] += 1
                    elif not is_leaky_vote and not golden_is_leaky:
                        metrics["golden_tn"] += 1
                    elif not is_leaky_vote and golden_is_leaky:
                        metrics["golden_fn"] += 1

            # Compile Final Scores
            total = self.num_posts

            avg_brier = round(metrics["brier_sum"] / total, 3)  # Lower is better

            scores = {
                "DS Reliability": reliability_scores.get(judge.judge_id, 0.0),
                "Brier Score (Calib)": avg_brier,
                "Hallucinations": metrics["hallucinations"],
            }

            # Add Golden Metrics if any golden labels were present
            golden_total = (
                metrics["golden_tp"]
                + metrics["golden_fp"]
                + metrics["golden_tn"]
                + metrics["golden_fn"]
            )
            if golden_total > 0:
                g_prec = (
                    metrics["golden_tp"] / (metrics["golden_tp"] + metrics["golden_fp"])
                    if (metrics["golden_tp"] + metrics["golden_fp"]) > 0
                    else 0.0
                )
                g_rec = (
                    metrics["golden_tp"] / (metrics["golden_tp"] + metrics["golden_fn"])
                    if (metrics["golden_tp"] + metrics["golden_fn"]) > 0
                    else 0.0
                )
                g_f1 = (
                    2 * (g_prec * g_rec) / (g_prec + g_rec)
                    if (g_prec + g_rec) > 0
                    else 0.0
                )

                scores.update(
                    {
                        "Golden Recall": round(g_rec * 100, 2),
                        "Golden F1": round(g_f1 * 100, 2),
                    }
                )

            judge_scores[judge.judge_id] = scores

        return judge_scores

    def calculate_krippendorff_alpha(self) -> float:
        """
        Calculates Krippendorff's Alpha for nominal data.
        """
        if not self.judges or len(self.judges) < 2:
            return 1.0

        # Matrix of judgments: (judges, items)
        data = np.zeros((self.num_judges, self.num_posts), dtype=int)
        for i, judge in enumerate(self.judges):
            for j in range(self.num_posts):
                data[i, j] = 1 if judge.judge_results[j].judgment.is_leaky else 0

        # Nominal categories
        categories = np.unique(data)
        num_categories = len(categories)
        if num_categories < 2:
            return 1.0

        # Coincidence matrix implementation
        coincidence = np.zeros((num_categories, num_categories))
        val_to_idx = {val: i for i, val in enumerate(categories)}

        for j in range(self.num_posts):
            item_data = data[:, j]
            for i1 in range(self.num_judges):
                for i2 in range(self.num_judges):
                    if i1 == i2:
                        continue
                    c1 = val_to_idx[item_data[i1]]
                    c2 = val_to_idx[item_data[i2]]
                    coincidence[c1, c2] += 1.0 / (self.num_judges - 1)

        n_c = np.sum(coincidence, axis=1)
        N = np.sum(n_c)
        if N == 0:
            return 1.0

        observed_disagreement = 0.0
        for i in range(num_categories):
            for j in range(num_categories):
                if i != j:
                    observed_disagreement += coincidence[i, j]
        observed_disagreement /= N

        expected_disagreement = 0.0
        for i in range(num_categories):
            for j in range(num_categories):
                if i != j:
                    expected_disagreement += n_c[i] * n_c[j]
        expected_disagreement /= N * (N - 1)

        if expected_disagreement == 0:
            return 1.0

        return 1.0 - (observed_disagreement / expected_disagreement)

    def get_disagreement_samples(self, n: int = 10) -> pd.DataFrame:
        if not self.judges:
            return pd.DataFrame()

        disagreement_data = []
        silver_labels, _ = self.calculate_dawid_skene_truth()

        for i in range(self.num_posts):
            post = self.judges[0].posts[i]
            row = {
                "post": post,
                "estimated_truth": "LEAKY" if silver_labels[i] else "SAFE",
            }
            leak_votes = 0
            safe_votes = 0

            for judge in self.judges:
                result = judge.judge_results[i]

                if self._verify_spans(result, post):
                    if result.judgment.is_leaky:
                        leak_votes += 1
                        row[judge.judge_id] = f"LEAKY ({result.judgment.confidence})"
                    else:
                        safe_votes += 1
                        row[judge.judge_id] = "SAFE"
                else:
                    row[judge.judge_id] = "HALLUCINATION"

            row["vote_split"] = f"{leak_votes}/{safe_votes}"
            # Disagreement score: higher is more disagreement
            row["disagreement_score"] = min(leak_votes, safe_votes)

            disagreement_data.append(row)

        df = pd.DataFrame(disagreement_data)
        if df.empty:
            return df
        return df.sort_values(by="disagreement_score", ascending=False).head(n)


class BootstrapEvaluator:
    """
    Wraps JudgeEvaluator to perform resampling (bootstrapping)
    to calculate Confidence Intervals for metrics.
    """

    def __init__(
        self,
        judges: List[JudgeEvaluation],
        golden_labels: Dict[str, str] | None = None,
    ):
        self.judges = judges
        self.golden_labels = golden_labels
        self.num_samples = len(judges[0].posts) if judges else 0

    def run_bootstrap(self, n_iterations: int = 100) -> pd.DataFrame:
        """
        Runs the bootstrap process and returns a DataFrame with Mean ± Std.
        """
        history = {j.judge_id: {} for j in self.judges}
        indices = np.arange(self.num_samples)

        logger.info(
            f"Starting Bootstrap: {n_iterations} iterations on {self.num_samples} samples."
        )

        for _ in tqdm(range(n_iterations), desc="Bootstrapping"):
            # Resample indices with replacement
            resampled_indices = np.random.choice(
                indices, size=self.num_samples, replace=True
            )

            # Create temporary Judge objects for this subset
            subset_judges = []
            for judge in self.judges:
                new_posts = [judge.posts[i] for i in resampled_indices]
                new_results = [judge.judge_results[i] for i in resampled_indices]

                subset_judges.append(
                    JudgeEvaluation(
                        judge_id=judge.judge_id,
                        judge_results=new_results,
                        posts=new_posts,
                    )
                )

            # Run Evaluation on Subset
            # Note: JudgeEvaluator works fine with resampled lists.
            # Golden labels dict lookup works automatically since it uses post content as key.
            evaluator = JudgeEvaluator(subset_judges, golden_labels=self.golden_labels)
            df_rank = evaluator.rank_judges()

            # Store Metrics
            for judge_id in df_rank.index:
                for col in df_rank.columns:
                    val = df_rank.loc[judge_id, col]
                    if col not in history[judge_id]:
                        history[judge_id][col] = []
                    history[judge_id][col].append(val)

        # Aggregate Results
        final_data = []
        for jid, metrics in history.items():
            row = {"Judge ID": jid}
            for metric, values in metrics.items():
                arr = np.array(values)
                mean = np.mean(arr)
                std = np.std(arr)

                # Format: "Mean ± Std"
                row[metric] = f"{mean:.3f} ± {std:.3f}"
                # Store numeric mean for sorting
                row[f"_sort_{metric}"] = mean
            final_data.append(row)

        df = (
            pd.DataFrame(final_data)
            .set_index("Judge ID")
            .sort_values("_sort_DS Reliability", ascending=False)
        )

        # Clean up sort columns
        df = df[[c for c in df.columns if not c.startswith("_sort_")]]
        return df


def load_results(results_dir: str, dataset_name: str) -> list[JudgeEvaluation]:
    """
    Loads all judge result CSVs for a given dataset name and filters for common samples.
    """
    pattern = os.path.join(results_dir, f"judge_results_*_{dataset_name}.csv")
    files = glob.glob(pattern)
    logger.info(
        f"Found {len(files)} result files for dataset '{dataset_name}' in '{results_dir}'"
    )

    judges = []

    for f in files:
        df = pd.read_csv(f)
        filename = os.path.basename(f)
        judge_id = filename.removeprefix("judge_results_").removesuffix(
            f"_{dataset_name}.csv"
        )

        judge_results = []
        for row in df.to_dict(orient="records"):
            evidence_spans = row.get("forensics.evidence_spans", [])
            if isinstance(evidence_spans, str):
                try:
                    evidence_spans = ast.literal_eval(evidence_spans)
                except (ValueError, SyntaxError):
                    evidence_spans = [evidence_spans] if evidence_spans else []
            elif not isinstance(evidence_spans, list):
                evidence_spans = []

            is_leaky = bool(row.get("judgment.is_leaky", False))

            judge_results.append(
                JudgeResult(
                    judgment=Judgment(
                        is_leaky=is_leaky,
                        confidence=row.get("judgment.confidence", "LOW"),
                        severity_score=row.get("judgment.severity_score", "LOW"),
                    ),
                    classification=(
                        Classification(
                            leakage_domain=row.get(
                                "classification.leakage_domain", "N/A"
                            ),
                            specific_mechanism=row.get(
                                "classification.specific_mechanism", ""
                            ),
                            is_novel_category=row.get(
                                "classification.is_novel_category", False
                            ),
                            definition=row.get("classification.definition", ""),
                        )
                        if is_leaky
                        else None
                    ),
                    forensics=(
                        Forensics(
                            evidence_spans=evidence_spans,
                            evidence_location=row.get(
                                "forensics.evidence_location", "N/A"
                            ),
                            pattern_abstraction=row.get(
                                "forensics.pattern_abstraction", ""
                            ),
                        )
                        if is_leaky
                        else None
                    ),
                )
            )
        judge_posts = [row["post"] for row in df.to_dict(orient="records")]
        judge_evaluations = JudgeEvaluation(
            judge_id=judge_id, judge_results=judge_results, posts=judge_posts
        )
        judges.append(judge_evaluations)

    # Filter out non-shared posts
    if not judges:
        return []

    sorted_common_posts = sorted(
        list(set.intersection(*[set(j.posts) for j in judges]))
    )
    logger.info(
        f"Found {len(sorted_common_posts)} common posts across {len(judges)} judges."
    )
    for judge in judges:
        post_map = {post: res for post, res in zip(judge.posts, judge.judge_results)}
        judge.posts = sorted_common_posts
        judge.judge_results = [post_map[post] for post in sorted_common_posts]

    return judges


def run_analysis(results_dir: str, dataset_name: str, disagreement_n: int = 10) -> None:
    """
    Main entry point for analyzing LLM Judge results.
    """
    console = Console()
    logger.info(f"Analyzing results for dataset: {dataset_name}")

    judges = load_results(results_dir, dataset_name)

    if not judges:
        logger.warning("No judges found.")
        return

    # Load golden labels if they exist
    golden_labels = {}
    golden_path = os.path.join(results_dir, f"golden_labels_{dataset_name}.csv")
    if os.path.exists(golden_path):
        logger.info(f"Loading golden labels from {golden_path}")
        golden_df = pd.read_csv(golden_path)
        golden_labels = dict(zip(golden_df["post"], golden_df["manual_label"]))
        logger.info(f"Loaded {len(golden_labels)} golden labels.")

    # Standard Analysis
    evaluator = JudgeEvaluator(judges, golden_labels=golden_labels)
    alpha = evaluator.calculate_krippendorff_alpha()
    console.print(
        f"[bold]Krippendorff's Alpha (Inter-rater Reliability):[/bold] [green]{alpha:.4f}[/green]"
    )

    bootstrapper = BootstrapEvaluator(judges, golden_labels=golden_labels)
    stability_df = bootstrapper.run_bootstrap(n_iterations=100)

    b_table = Table(
        title="BOOTSTRAP STABILITY ANALYSIS",
        show_header=True,
        header_style="bold cyan",
        title_style="bold underline",
    )
    b_table.add_column("Judge ID", style="cyan", no_wrap=True)
    for col in stability_df.columns:
        display_name = (
            col.replace("Reliability", "Rel.")
            .replace("Brier Score (Calib)", "Brier (Cal)")
            .replace("Hallucinations", "Halluc.")
            .replace("Golden", "G.")
        )
        style = "green" if "Golden" in col or "Reliability" in col else "white"
        b_table.add_column(display_name, justify="right", style=style)

    for judge_id, row in stability_df.iterrows():
        b_table.add_row(str(judge_id), *[str(val) for val in row])

    console.print(b_table)

    # Disagreement Analysis
    disagreement_df = evaluator.get_disagreement_samples(n=disagreement_n)
    if not disagreement_df.empty:
        output_path = os.path.join(
            results_dir, f"disagreement_samples_{dataset_name}.csv"
        )
        disagreement_df.to_csv(output_path, index=False)
        logger.info(
            f"Saved {len(disagreement_df)} samples with most disagreement to {output_path}"
        )
