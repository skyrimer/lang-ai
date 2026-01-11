"""
Script to run multiple LLM Judges on a dataset.
"""

import hydra
from omegaconf import DictConfig

from src.lang_ai.judge.agent import run_multi_judge


@hydra.main(config_path="configs", config_name="llmaj_config", version_base=None)
def main(cfg: DictConfig) -> None:
    run_multi_judge(
        input_csv=cfg.multi_judge.input,
        models=cfg.multi_judge.models,
        system_prompt_path=cfg.multi_judge.system_prompt,
        results_dir=cfg.multi_judge.results_dir,
    )


if __name__ == "__main__":
    main()
