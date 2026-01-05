import sys
from pathlib import Path

import hydra
import pandas as pd
from omegaconf import DictConfig

from src.lang_ai.core.logger import setup_logging

logger = setup_logging(__name__)


def sample_posts(
    input_path: str, output_path: str, n: int = 5000, random_seed: int = 42
):
    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        logger.error(f"Input file {input_path} not found.")
        sys.exit(1)

    logger.info(f"Sampling {n} posts from {input_path}...")
    df = pd.read_csv(input_path, low_memory=False)

    if len(df) < n:
        logger.warning(
            f"Dataset has only {len(df)} rows, which is less than the requested sample size of {n}."
        )
        sample_df = df
    else:
        logger.info(f"Sampling {n} posts...")
        sample_df = df.sample(n=n, random_state=random_seed)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    sample_df.to_csv(output_path, index=False)
    logger.info(f"Successfully saved {len(sample_df)} posts to {output_path}")


@hydra.main(config_path="configs", config_name="llmaj_config", version_base=None)
def main(cfg: DictConfig):
    sample_posts(
        cfg.sample.input, cfg.sample.output, cfg.sample.n, cfg.sample.random_seed
    )


if __name__ == "__main__":
    main()
