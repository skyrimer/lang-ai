import logging
from pathlib import Path
from typing import Any

import logfire
import pandas as pd
from pydantic_ai import Agent
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.models import Model
from pydantic_ai.models.cerebras import CerebrasModel
from pydantic_ai.models.groq import GroqModel
from pydantic_ai.models.huggingface import HuggingFaceModel
from pydantic_ai.models.openrouter import OpenRouterModel
from pydantic_ai.providers.cerebras import CerebrasProvider
from pydantic_ai.providers.groq import GroqProvider
from pydantic_ai.providers.huggingface import HuggingFaceProvider
from pydantic_ai.providers.openrouter import OpenRouterProvider
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from tqdm.auto import tqdm

from src.lang_ai.core.logger import setup_logging
from src.lang_ai.core.models import JudgeResult
from src.lang_ai.core.utils import get_env_var

logfire.configure()
logfire.instrument_pydantic_ai()
logger = setup_logging(__name__)


def is_retryable_error(exception: Exception) -> bool:
    """Check if the error is retryable (402, 429, 5xx)."""
    match exception:
        case ModelHTTPError():
            return (
                exception.status_code in {402, 429}
                or 500 <= exception.status_code < 600
            )
        case _:
            return False


class LLMJudgeAgent(Agent):
    """LLM-based judge for evaluating text posts, extending pydantic_ai Agent."""

    def __init__(
        self,
        model: str,
        system_prompt_path: str | Path = "",
        output_retries: int = 3,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the LLM Judge Agent.

        Args:
            model (str): Model name or alias.
            system_prompt_path (str | Path): Path to the system prompt text file. Defaults to "".
            output_retries (int): Number of retries for output generation. Defaults to 1.
            **kwargs (Any): Additional arguments passed to Agent.__init__.
        """
        super().__init__(
            model=self._load_model(model),
            output_type=JudgeResult,
            system_prompt=self._load_system_prompt(system_prompt_path),
            output_retries=output_retries,
            **kwargs,
        )
        self.model_name = self.model.model_name

    @staticmethod
    def _load_model(model: str) -> Model | str:
        """
        Loads the model based on the provided model name or alias.

        Args:
            model (str): The model name or alias (e.g., 'huggingface:model:provider').

        Returns:
            Model | str: The loaded model object or the model name string.
        """
        split = model.split(":", 1)
        if len(split) == 1:
            return model
        prefix, model_name = split
        match prefix:
            case "huggingface":
                model_name, provider = model_name.split(":", 1)
                return HuggingFaceModel(
                    model_name,
                    provider=HuggingFaceProvider(
                        api_key=get_env_var("HF_TOKEN"), provider_name=provider
                    ),
                )
            case "cerebras":
                return CerebrasModel(
                    model_name,
                    provider=CerebrasProvider(api_key=get_env_var("CEREBRAS_API_KEY")),
                )
            case "groq":
                return GroqModel(
                    model_name,
                    provider=GroqProvider(api_key=get_env_var("GROQ_API_KEY")),
                )
            case "openrouter":
                return OpenRouterModel(
                    model_name,
                    provider=OpenRouterProvider(
                        api_key=get_env_var("OPENROUTER_API_KEY")
                    ),
                )
            case _:
                return model

    @staticmethod
    def _load_system_prompt(path: str | Path = "") -> str:
        """
        Loads the system prompt from a text file.

        Args:
            path (str | Path): Path to the system prompt file. Defaults to "".

        Returns:
            str: The system prompt as a string.

        Raises:
            FileNotFoundError: If the system prompt file does not exist.
        """
        if not path:
            return ""

        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"System prompt file not found: {path}")

        return path.read_text(encoding="utf-8")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=5, max=300),
        retry=retry_if_exception(is_retryable_error),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def judge(self, text: str) -> JudgeResult:
        """
        Judges a single text post for data leakage.

        Args:
            text (str): The text content to evaluate.

        Returns:
            JudgeResult: The result of the evaluation.
        """
        result = self.run_sync(text)
        return result.output


def save_judge_results(
    result: JudgeResult,
    original_row: pd.Series | pd.DataFrame,
    output_path: str | Path,
) -> None:
    """
    Saves the judge results along with the original data to a CSV file.

    Args:
        result (JudgeResult): The judge result object.
        original_row (pd.Series | pd.DataFrame): The original row(s) corresponding to the judged text.
        output_path (str | Path): The path where the results should be saved.
    """
    output_path = Path(output_path)

    results_df = pd.json_normalize(result.model_dump())

    if isinstance(original_row, pd.Series):
        original_df = pd.DataFrame([original_row]).reset_index(drop=True)
    else:
        original_df = original_row.reset_index(drop=True)

    combined_df = pd.concat([original_df, results_df], axis=1)

    # Create parent directories if they don't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Append to CSV if it exists, otherwise create it
    header = not output_path.exists()
    combined_df.to_csv(output_path, mode="a", index=False, header=header)
    logger.info(f"Results appended to {output_path}")


def run_judge_pipeline(
    input_csv: str | Path,
    results_dir: str | Path,
    model: str,
    system_prompt_path: str | Path,
    text_column: str = "post",
) -> None:
    """
    Runs the LLM judge on a dataset with incremental saving and resume capability.

    Args:
        input_csv (str | Path): Path to the input preprocessed dataset CSV.
        results_dir (str | Path): Directory where model results will be saved.
        model (str): Model name or alias to use for judging.
        system_prompt_path (str | Path): Path to the system prompt file.
        text_column (str): The name of the column containing the text to judge. Defaults to "post".
    """
    agent = LLMJudgeAgent(model=model, system_prompt_path=system_prompt_path)

    input_csv = Path(input_csv)
    sanitized_model = (
        str(agent.model_name).replace(":", "_").replace("/", "_").replace("\\", "_")
    )
    output_csv = (
        Path(results_dir) / f"judge_results_{sanitized_model}_{input_csv.stem}.csv"
    )
    logger.info(f"Saving results to {output_csv}")

    if not input_csv.exists():
        logger.error(f"Input file not found: {input_csv}")
        return

    df = pd.read_csv(input_csv)

    logger.info(f"Loaded {len(df)} unique rows from {input_csv}")

    processed_texts = set()
    if output_csv.exists():
        processed_df = pd.read_csv(output_csv)
        processed_texts = set(processed_df[text_column].astype(str).tolist())
        logger.info(f"Found {len(processed_texts)} already processed rows.")

    # Filter out already processed rows
    df = df[~df[text_column].astype(str).isin(processed_texts)]
    logger.info(f"Remaining rows to process: {len(df)}")

    if df.empty:
        logger.info("No new posts to judge.")
        return

    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Judging posts ({model})"):
        text = str(row[text_column])
        result = agent.judge(text)
        save_judge_results(result, row, output_csv)

    logger.info(f"Pipeline completed for model: {model}")


def run_multi_judge(
    input_csv: str | Path,
    models: list[str],
    system_prompt_path: str | Path,
    results_dir: str | Path,
) -> None:
    """
    Runs multiple LLM judges on the same input dataset.

    Args:
        input_csv: Path to the input CSV file.
        models: List of model names/aliases.
        system_prompt_path: Path to the system prompt file.
        results_dir: Directory where individual model results will be saved.
    """
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    input_csv = Path(input_csv)
    if not input_csv.exists():
        logger.error(f"Input file not found: {input_csv}")
        return

    for model in models:
        logger.info(f"Starting judge pipeline with model: {model}")
        run_judge_pipeline(
            input_csv=input_csv,
            results_dir=results_dir,
            model=model,
            system_prompt_path=system_prompt_path,
        )

    logger.info("All models processed.")
