"""
Judge Pipeline: Creates samples and runs LLM judges for quality assessment.

This pipeline:
1. Loads input data (raw or preprocessed)
2. Creates a stratified sample (optional, configurable size)
3. Runs multiple LLM judges on the sample
4. Analyzes judge results (Dawid-Skene, Krippendorff's Alpha, Bootstrap)
5. Identifies best model
6. Runs best model on full dataset
"""

import json
from pathlib import Path
from typing import Optional

import hydra
import pandas as pd
from omegaconf import DictConfig
from rich.console import Console
from rich.table import Table

from src.lang_ai.core.logger import setup_logging
from src.lang_ai.judge.agent import run_judge_pipeline, run_multi_judge
from src.lang_ai.judge.judge_analysis import (
    BootstrapEvaluator,
    JudgeEvaluator,
    load_results,
)
from src.lang_ai.judge.sampling import create_sample

logger = setup_logging(__name__)
console = Console()


def extract_dataset_name(file_path: Path) -> str:
    """
    Extract dataset name from file path.

    Args:
        file_path: Path to dataset file

    Returns:
        Dataset name (filename without extension)
    """
    return file_path.stem


def run_judge_analysis(
    results_dir: Path,
    dataset_name: str,
    disagreement_n: int = 25,
    bootstrap_iterations: int = 100,
    golden_labels_path: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Run analysis on multi-judge results and identify best model.

    Args:
        results_dir: Directory containing judge results
        dataset_name: Name of dataset (for loading results)
        disagreement_n: Number of disagreement samples to save
        bootstrap_iterations: Number of bootstrap iterations
        golden_labels_path: Optional path to golden labels CSV

    Returns:
        Bootstrap stability dataframe
    """
    logger.info("JUDGE ANALYSIS")

    # Load judge results
    judges = load_results(str(results_dir), dataset_name)

    if not judges:
        logger.error("No judges found for analysis")
        raise ValueError("No judges found for analysis")

    logger.info(f"Loaded results from {len(judges)} judges")

    # Load golden labels if provided
    golden_labels = {}
    if golden_labels_path and golden_labels_path.exists():
        logger.info(f"Loading golden labels from {golden_labels_path}")
        golden_df = pd.read_csv(golden_labels_path)
        golden_labels = dict(zip(golden_df["post"], golden_df["manual_label"]))
        logger.info(f"Loaded {len(golden_labels)} golden labels")

    # Run evaluation
    evaluator = JudgeEvaluator(judges, golden_labels=golden_labels)

    # Krippendorff's Alpha
    alpha = evaluator.calculate_krippendorff_alpha()

    # Bootstrap stability analysis
    bootstrapper = BootstrapEvaluator(judges, golden_labels=golden_labels)
    stability_df = bootstrapper.run_bootstrap(n_iterations=bootstrap_iterations)

    # Display results table
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

    console.print(
        f"[bold]Krippendorff's Alpha (Inter-rater Reliability):[/bold] [green]{alpha:.4f}[/green]"
    )
    console.print(b_table)

    # Save disagreement samples
    disagreement_df = evaluator.get_disagreement_samples(n=disagreement_n)
    if not disagreement_df.empty:
        output_path = results_dir / f"disagreement_samples_{dataset_name}.csv"
        disagreement_df.to_csv(output_path, index=False)
        logger.info(
            f"Saved {len(disagreement_df)} samples with most disagreement to {output_path}"
        )

    # Save analysis report
    analysis_report = {
        "dataset_name": dataset_name,
        "num_judges": len(judges),
        "krippendorff_alpha": alpha,
        "bootstrap_iterations": bootstrap_iterations,
        "has_golden_labels": len(golden_labels) > 0,
    }
    report_path = results_dir / f"judge_analysis_report_{dataset_name}.json"
    with open(report_path, "w") as f:
        json.dump(analysis_report, f, indent=2)
    logger.info(f"Saved analysis report to {report_path}")

    return stability_df


def run_best_model_on_full_dataset(
    best_model: str,
    full_dataset_path: Path,
    results_dir: Path,
    system_prompt_path: str,
) -> None:
    """
    Run best model on full dataset.

    Args:
        best_model: Model identifier (best from analysis)
        full_dataset_path: Path to full dataset
        results_dir: Directory for results
        system_prompt_path: Path to system prompt
    """
    logger.info(f"Running best model: {best_model}")
    logger.info(f"Full dataset: {full_dataset_path}")

    # Run judge pipeline on full dataset
    run_judge_pipeline(
        input_csv=full_dataset_path,
        results_dir=results_dir,
        model=best_model,
        system_prompt_path=system_prompt_path,
    )

    logger.info("Best model run completed")


def run_judge_pipeline_from_config(cfg: DictConfig) -> None:
    """
    Runs the complete judge pipeline using Hydra configuration.

    Pipeline Steps:
    1. Sample creation (optional)
    2. Multi-judge analysis on sample
    3. Judge evaluation & best model selection
    4. Best model run on full dataset

    Args:
        cfg: Hydra configuration object
    """
    logger.info("JUDGE PIPELINE")

    input_path = Path(cfg.sampling.input)
    results_dir = Path(cfg.multi_judge.results_dir)

    # STEP 1 & 2: Sampling and Multi-Judge
    # Determine data source for judges
    if cfg.sampling.enabled:
        # Create sample first
        sample_output_path = Path(cfg.sampling.output)
        logger.info(
            f"Creating sample of size {cfg.sampling.sample_size} from {input_path}"
        )

        create_sample(
            input_path=input_path,
            output_path=sample_output_path,
            sample_size=cfg.sampling.sample_size,
            random_seed=cfg.sampling.random_seed,
            stratify_column=cfg.sampling.get("stratify_column", None),
        )

        judge_input = sample_output_path
        dataset_name = extract_dataset_name(sample_output_path)
    else:
        # Use full dataset
        judge_input = input_path
        dataset_name = extract_dataset_name(input_path)
        logger.info("Sampling disabled - using full dataset")

    if cfg.multi_judge.enabled:
        # Run multi-judge
        logger.info(f"Running multi-judge on {judge_input}")
        run_multi_judge(
            input_csv=judge_input,
            models=cfg.multi_judge.models,
            system_prompt_path=cfg.multi_judge.system_prompt,
            results_dir=results_dir,
        )
    else:
        logger.info("Multi-judge disabled in config")

    # STEP 3: Analysis (if enabled)
    if cfg.analysis.enabled and len(cfg.multi_judge.models) > 1:
        golden_labels_path = None
        if cfg.analysis.golden_labels_path:
            golden_labels_path = Path(cfg.analysis.golden_labels_path)

        run_judge_analysis(
            results_dir=results_dir,
            dataset_name=dataset_name,
            disagreement_n=cfg.analysis.disagreement_n,
            bootstrap_iterations=cfg.analysis.bootstrap_iterations,
            golden_labels_path=golden_labels_path,
        )
    else:
        logger.info("Analysis disabled in config")

    # STEP 4: Best Model on Full Dataset (if enabled)
    if cfg.best_model.run_on_full_dataset:
        # Determine best model
        best_model = cfg.best_model.model
        logger.info(f"Using configured best model: {best_model}")

        # Determine full dataset path
        full_dataset_path = input_path
        if cfg.best_model.full_dataset_path:
            full_dataset_path = cfg.best_model.full_dataset_path

        # Only run if not already processed
        if cfg.sampling.enabled or judge_input != full_dataset_path:
            run_best_model_on_full_dataset(
                best_model=best_model,
                full_dataset_path=full_dataset_path,
                results_dir=results_dir,
                system_prompt_path=cfg.multi_judge.system_prompt,
            )
        else:
            logger.info(
                "Skipping best model run - already ran on full dataset in Step 2"
            )
    else:
        logger.info("Best model full dataset run disabled in config")

    logger.info("JUDGE PIPELINE COMPLETED SUCCESSFULLY")


@hydra.main(config_path="configs", config_name="judge_config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Hydra entry point for judge pipeline.

    Usage examples:
        # Run full pipeline
        python run_judge_pipeline.py

        # Disable sampling (use full dataset)
        python run_judge_pipeline.py sampling.enabled=false

        # Disable analysis step
        python run_judge_pipeline.py analysis.enabled=false

        # Disable best model full dataset run
        python run_judge_pipeline.py best_model.run_on_full_dataset=false

        # Override sample size
        python run_judge_pipeline.py sampling.sample_size=1000

        # Override models for multi-judge
        python run_judge_pipeline.py multi_judge.models=[model1,model2,model3]

        # Override input path
        python run_judge_pipeline.py sampling.input=custom_data/input.csv

        # Multiple overrides
        python run_judge_pipeline.py sampling.enabled=false analysis.enabled=false

        # Use different config
        python run_judge_pipeline.py --config-name alternative_judge_config
    """
    run_judge_pipeline_from_config(cfg)


if __name__ == "__main__":
    main()
