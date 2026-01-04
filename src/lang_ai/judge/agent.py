from pathlib import Path
from typing import Any

import pandas as pd
from tqdm.auto import tqdm
from pydantic_ai import Agent
from src.lang_ai.core.models import JudgeResult
from pydantic_ai.models.huggingface import HuggingFaceModel
from pydantic_ai.providers.huggingface import HuggingFaceProvider
from src.lang_ai.core.utils import get_env_var
from src.lang_ai.core.logger import setup_logging

logger = setup_logging(__name__)


class LLMJudgeAgent(Agent):
    """LLM-based judge for evaluating text posts, extending pydantic_ai Agent."""

    def __init__(
        self,
        model: str,
        system_prompt_path: str | Path = "",
        output_retries: int = 2,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the LLM Judge Agent.

        Args:
            model (str): Model name or alias.
            system_prompt_path (str | Path): Path to the system prompt text file. Defaults to "".
            output_retries (int): Number of retries for output generation. Defaults to 2.
            **kwargs (Any): Additional arguments passed to Agent.__init__.
        """
        super().__init__(
            model=self._load_model(model),
            output_type=JudgeResult,
            system_prompt=self._load_system_prompt(system_prompt_path),
            output_retries=output_retries,
            **kwargs,
        )

    @staticmethod
    def _load_model(model: str) -> HuggingFaceModel | str:
        """
        Loads the model based on the provided model name or alias.

        Args:
            model (str): The model name or alias (e.g., 'huggingface:model:provider').

        Returns:
            HuggingFaceModel | str: The loaded model object or the model name string.
        """
        hf_alias = "huggingface:"
        if model.startswith(hf_alias):
            # Format: huggingface:model_name:provider
            parts = model[len(hf_alias) :].split(":")
            if len(parts) >= 2:
                model_provider = parts[-1]
                model_name = ":".join(parts[:-1])
                return HuggingFaceModel(
                    model_name,
                    provider=HuggingFaceProvider(
                        api_key=get_env_var("HF_API_KEY"), provider_name=model_provider
                    ),
                )
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

    async def judge(self, text: str) -> JudgeResult:
        """
        Judges a single text post for data leakage.

        Args:
            text (str): The text content to evaluate.

        Returns:
            JudgeResult: The result of the evaluation.
        """
        result = await self.run(text)
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


async def run_judge_pipeline(
    input_csv: str | Path,
    output_csv: str | Path,
    model: str,
    system_prompt_path: str | Path,
    text_column: str = "post",
) -> None:
    """
    Runs the LLM judge on a dataset with incremental saving and resume capability.

    Args:
        input_csv (str | Path): Path to the input preprocessed dataset CSV.
        output_csv (str | Path): Path where the results CSV should be saved.
        model (str): Model name or alias to use for judging.
        system_prompt_path (str | Path): Path to the system prompt file.
        text_column (str): The name of the column containing the text to judge. Defaults to "post".
    """
    input_csv = Path(input_csv)
    output_csv = Path(output_csv)

    if not input_csv.exists():
        logger.error(f"Input file not found: {input_csv}")
        return

    df = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(df)} rows from {input_csv}")

    processed_texts = set()
    if output_csv.exists():
        try:
            existing_df = pd.read_csv(output_csv)
            if text_column in existing_df.columns:
                processed_texts = set(existing_df[text_column].astype(str).tolist())
                logger.info(f"Found {len(processed_texts)} already processed rows.")
        except Exception as e:
            logger.warning(f"Could not read existing output file: {e}. Starting fresh.")

    agent = LLMJudgeAgent(model=model, system_prompt_path=system_prompt_path)

    for i, row in tqdm(df.iterrows(), total=len(df), desc="Judging posts"):
        text = str(row[text_column])

        if text in processed_texts:
            continue

        try:
            result = await agent.judge(text)
            save_judge_results(result, row, output_csv)
            processed_texts.add(text)
        except Exception as e:
            logger.error(f"Error judging row {i}: {e}")
            continue

    logger.info("Pipeline completed.")
