"""
Model Evaluation Pipeline: Full data processing and model evaluation.

This pipeline orchestrates:
1. Preprocessing (pollution removal)
2. Deleakage (leakage pattern removal)
3. Evaluation (baseline vs sanitized comparison)
"""

from pathlib import Path

import hydra
from omegaconf import DictConfig

from src.lang_ai.core.logger import setup_logging
from src.lang_ai.data.deleakage_pipeline import deleakage_pipeline
from src.lang_ai.data.preprocessor import preprocess_data
from src.lang_ai.evaluation.stability_evaluation import main as run_stability_evaluation

logger = setup_logging(__name__)


def run_model_pipeline_from_config(cfg: DictConfig) -> None:
    """
    Run the complete model evaluation pipeline using Hydra configuration.

    Pipeline Steps:
    1. Preprocessing (pollution removal)
    2. Deleakage (leakage pattern removal)
    3. Evaluation (baseline vs sanitized comparison)

    Args:
        cfg: Hydra configuration object
    """
    logger.info("MODEL EVALUATION PIPELINE")

    # Extract paths from config
    raw_data_path = Path(cfg.paths.raw_data)
    preprocessed_path = Path(cfg.paths.preprocessed)
    deleaked_path = Path(cfg.paths.deleaked)
    results_dir = Path(cfg.paths.results)

    # Step 1: Preprocessing (pollution removal)
    if cfg.pipeline.run_preprocessing:
        logger.info("PREPROCESSING (Pollution Removal)")
        preprocess_data(data_url=raw_data_path, output_path=preprocessed_path)
    else:
        logger.info("Skipping preprocessing step")
        logger.info(f"Using existing preprocessed data: {preprocessed_path}")

    # Step 2: Deleakage (leakage pattern removal)
    if cfg.pipeline.run_deleakage:
        logger.info("DELEAKAGE (Leakage Pattern Removal)")
        deleakage_pipeline(input_path=preprocessed_path, output_path=deleaked_path)
    else:
        logger.info("Skipping deleakage step")
        logger.info(f"Using existing deleaked data: {deleaked_path}")

    # Step 3: Evaluation (baseline vs sanitized comparison)
    if cfg.pipeline.run_evaluation:
        logger.info("EVALUATION (Baseline vs Sanitized Comparison)")

        logger.info(f"Baseline corpus: {preprocessed_path}")
        logger.info(f"Sanitized corpus: {deleaked_path}")
        logger.info(f"Results directory: {results_dir}")
        run_stability_evaluation(cfg)
    else:
        logger.info("Skipping evaluation step")

    logger.info("MODEL EVALUATION PIPELINE COMPLETED")
    logger.info(f"Results saved to: {results_dir}")
    logger.info("Generated files:")
    logger.info(f"  - {results_dir / 'baseline_evaluation.json'}")
    logger.info(f"  - {results_dir / 'sanitized_evaluation.json'}")
    logger.info(f"  - {results_dir / 'comparison_report.json'}")
    logger.info(f"  - {results_dir / 'comparison_summary.csv'}")
    logger.info(
        f"  - {results_dir / 'feature_analysis' / 'baseline_top_features.json'}"
    )
    logger.info(
        f"  - {results_dir / 'feature_analysis' / 'sanitized_top_features.json'}"
    )


@hydra.main(config_path="configs", config_name="model_config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Hydra entry point for model pipeline.

    Usage examples:
        # Run full pipeline
        python run_model_pipeline.py

        # Skip preprocessing
        python run_model_pipeline.py pipeline.run_preprocessing=false

        # Skip deleakage
        python run_model_pipeline.py pipeline.run_deleakage=false

        # Only run evaluation
        python run_model_pipeline.py pipeline.run_preprocessing=false pipeline.run_deleakage=false

        # Override paths
        python run_model_pipeline.py paths.raw_data=custom_data/input.csv

        # Multiple overrides
        python run_model_pipeline.py paths.raw_data=data/custom.csv paths.results=results/experiment_001

        # Use different config
        python run_model_pipeline.py --config-name alternative_model_config
    """
    run_model_pipeline_from_config(cfg)


if __name__ == "__main__":
    main()
