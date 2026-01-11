"""
Script to run the analysis on LLM Judge results.
"""

import hydra
from omegaconf import DictConfig

from src.lang_ai.judge.judge_analysis import run_analysis


@hydra.main(config_path="configs", config_name="llmaj_config", version_base=None)
def main(cfg: DictConfig) -> None:
    run_analysis(
        results_dir=cfg.analysis.results_dir,
        dataset_name=cfg.analysis.dataset_name,
        disagreement_n=cfg.analysis.disagreement_n,
    )


if __name__ == "__main__":
    main()
