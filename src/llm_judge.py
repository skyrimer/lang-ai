from pathlib import Path

import pandas as pd
from pydantic_ai import Agent
from src.models import JudgeResult


class LLMJudgeAgent(Agent):
    """LLM-based judge for evaluating text posts, extending pydantic_ai Agent."""

    def __init__(
        self,
        model: str,
        system_prompt_path: str | Path = "",
        output_retries: int = 3,
        **kwargs,
    ):
        """
        Initialize the LLM Judge Agent.

        Args:
            model: Model name (string) or a Model object (passed to Agent).
            system_prompt_path: Path to the system prompt text file.
            output_retries: Number of retries for output generation.
            **kwargs: Additional arguments passed to Agent.__init__.
        """
        super().__init__(
            model=model,
            output_type=JudgeResult,
            system_prompt=self.load_system_prompt(system_prompt_path),
            output_retries=output_retries,
            **kwargs,
        )

    @staticmethod
    def load_system_prompt(path: str | Path = "") -> str:
        """
        Load system prompt from file.

        Args:
            path: Path to the system prompt file.

        Returns:
            System prompt as a string.
        """
        if not path:
            return ""

        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"System prompt file not found: {path}")

        return path.read_text(encoding="utf-8")

    async def judge(self, text: str) -> JudgeResult:
        """
        Judge a single text post.

        Args:
            text: The text to evaluate.

        Returns:
            JudgeResult if successful, None if an error occurs.
        """
        result = await self.run(text)
        return result.output


def save_judge_results(
    result: JudgeResult,
    original_df: pd.DataFrame,
    output_path: str | Path,
) -> None:
    """
    Save the judge results along with the original data.

    Args:
        result: JudgeResult object.
        original_df: Original DataFrame with input data.
        output_path: Path where to save the combined results.
    """
    output_path = Path(output_path)

    results_df = pd.json_normalize(result.model_dump())

    combined_df = pd.concat([original_df.reset_index(drop=True), results_df], axis=1)

    # Create parent directories if they don't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(output_path, index=False)
    print(f"Results saved to {output_path}")
