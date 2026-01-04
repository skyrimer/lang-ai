import asyncio
import logfire
from src.data_preprocessor import preprocess_data
from src.llm_judge import run_judge_pipeline
from src.utils import get_env_var
# Configure logfire
logfire.configure()
logfire.instrument_pydantic_ai()


async def run_pipeline() -> None:
    """
    Main entry point for the lang-ai project.
    Runs the full data preprocessing and judge pipeline.
    """
    # 1. Preprocess data (optional, can be skipped if already exists)
    preprocess_data()

    # 2. Run LLM Judge
    model = get_env_var("LLM_MODEL")
    system_prompt = "prompts/llmaj_prompt_by_grazie"
    input_csv = "preprocessed_data/preprocessed_data.csv"
    output_csv = "results/judge_results.csv"

    print(f"Starting judge pipeline with model: {model}")
    await run_judge_pipeline(
        input_csv=input_csv,
        output_csv=output_csv,
        model=model,
        system_prompt_path=system_prompt,
    )


def main() -> None:
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
