import hydra
from omegaconf import DictConfig

from src.lang_ai.data.preprocessor import preprocess_data
from src.lang_ai.judge.agent import run_multi_judge


def run_pipeline(cfg: DictConfig) -> None:
    """
    Main entry point for the lang-ai project.
    Runs the full data preprocessing and multi-model judge pipeline.
    """
    # 1. Preprocess data (optional, can be skipped if already exists)
    preprocess_data()

    # 2. Run LLM Judges
    run_multi_judge(
        input_csv=cfg.multi_judge.input,
        models=cfg.multi_judge.models,
        system_prompt_path=cfg.multi_judge.system_prompt,
        results_dir=cfg.multi_judge.results_dir,
    )


@hydra.main(config_path="configs", config_name="llmaj_config", version_base=None)
def main(cfg: DictConfig) -> None:
    run_pipeline(cfg)


if __name__ == "__main__":
    main()
