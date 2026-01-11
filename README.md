# Lang-AI: Language and AI Project

A Python project for stylometric analysis, data preprocessing, and LLM-based evaluation of Reddit posts.

## Project Overview

This project provides a robust pipeline for cleaning and preprocessing text data (specifically Reddit posts) to prepare it for stylometric analysis. It includes advanced features for text normalization, multi-stage filtering, similarity-based deduplication, and an LLM-as-a-Judge infrastructure for data leakage detection.

## Key Features

- **Advanced Text Normalization**: High-precision regex-based replacement of non-stylometric elements (URLs, emails, user mentions, IP addresses) with standardized tokens.
- **Multi-Stage Preprocessing Pipeline**: A configurable pipeline that includes:
    - Typo correction in headers.
    - Non-standard word ratio filtering.
    - Repetitive wording removal.
    - Length and quality normalization.
    - Pattern-based "messy" post filtering (bots, ads, etc.).
- **Similarity-Based Deduplication**: Identification and resolution of near-duplicate posts using Jaccard similarity and connected components clustering.
- **LLM-as-a-Judge**: A powerful evaluation framework using `pydantic-ai` to score posts for data leakage and pollution across multiple LLM providers (OpenAI, Google, Groq, Cerebras, OpenRouter, and Hugging Face).
- **Advanced Analysis**: Sophisticated evaluation metrics including Dawid-Skene truth estimation, Brier score ranking, Krippendorff's alpha for inter-annotator agreement, and bootstrap-based confidence intervals.
- **Observability**: Integrated with **Logfire** for real-time tracking, logging, and performance monitoring of the entire pipeline.
- **Resilient Execution**: Incremental processing and resume capabilities for the LLM Judge to handle large datasets efficiently.
- **Labeling App**: Built-in Streamlit application for manual verification and labeling of disagreement samples.

## Installation

This project uses `uv` for lightning-fast dependency management.

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd lang-ai
   ```

2. **Set up the environment**:
   Create a `.env` file in the root directory and add the required environment variables:
   ```env
   # LLM Judge API Keys
   OPENAI_API_KEY=<your-openai-key>
   GOOGLE_API_KEY=<your-google-key>
   GROQ_API_KEY=<your-groq-key>
   CEREBRAS_API_KEY=<your-cerebras-key>
   OPENROUTER_API_KEY=<your-openrouter-key>
   HF_TOKEN=<your-huggingface-token>

   # Observability
   LOGFIRE_TOKEN=<your-logfire-token>
   ```

3. **Prepare the data**:
   Place the `political_leaning.csv` dataset in the `raw_data/assignment_data/` directory:
   ```bash
   mkdir -p raw_data/assignment_data
   # Move your political_leaning.csv there
   ```

4. **Install dependencies**:
   ```bash
   make sync
   ```

5. **Configure Observability (Optional)**:
   ```bash
   make logfire-auth
   ```

## Usage

### Analysis and Labeling

1. Run the analysis to generate disagreement samples:
   ```bash
   uv run run_analysis.py
   ```

2. Label the disagreement samples using the Streamlit app:
   ```bash
   uv run streamlit run src/lang_ai/judge/labeling_app.py
   ```

3. Re-run the analysis to use the golden labels:
   ```bash
   uv run run_analysis.py
   ```

The project uses **Hydra** for configuration management. All parameters are defined in `configs/llmaj_config.yaml` and can be overridden via command-line arguments.

### 1. Run the Full Pipeline
The easiest way to run both preprocessing and the LLM judge for a set of models is through `main.py`:
```bash
uv run main.py
```
To override the models or the input dataset:
```bash
uv run main.py 'multi_judge.models=[openai:gpt-4o]' multi_judge.input=preprocessed_data/sample_500.csv
```

### 2. Individual Components
- **Run Preprocessor Only**:
  ```bash
  uv run python -m src.lang_ai.data.preprocessor
  ```
- **Sample Posts**:
  Sample a subset of posts for later evaluation (configurable in `configs/llmaj_config.yaml`):
  ```bash
  uv run sample_posts.py sample.n=500
  ```

- **Run Multiple Judges**:
  Run multiple LLM models on a specific dataset (e.g., a sample):
  ```bash
  uv run run_multi_judge.py multi_judge.input=preprocessed_data/sample_500.csv
  ```
  To override models from the command line:
  ```bash
  uv run run_multi_judge.py 'multi_judge.models=[google:gemini-3-flash-preview, openrouter:anthropic/claude-3-haiku]'
  ```
  Results for each model will be saved in the `results/` directory as `judge_results_<model_name>_<input_dataset_filename>.csv`.

## Project Structure

- `src/lang_ai/`: Core source code.
    - `data/`: Data handling and transformation.
        - `preprocessor.py`: Orchestration of the cleaning pipeline.
        - `normalizer.py`: Text cleaning and tokenization logic.
        - `filters.py`: Regex patterns for identifying "messy" content.
    - `judge/`: LLM evaluation logic.
        - `agent.py`: `pydantic-ai` agent implementation and pipeline.
        - `judge_analysis.py`: Evaluation metrics and agreement analysis.
        - `labeling_app.py`: Streamlit app for manual labeling.
    - `analysis/`: Algorithmic data analysis.
        - `similarity.py`: Jaccard similarity and clustering utilities.
    - `core/`: Shared infrastructure.
        - `models.py`: Structured data definitions (Pydantic).
        - `logger.py`: Logging setup.
        - `utils.py`: Environment and path utilities.
- `main.py`: Unified entry point for the end-to-end workflow.
- `run_multi_judge.py`: Script for running multiple models in parallel.
- `run_analysis.py`: Script for calculating metrics and generating disagreement samples.
- `sample_posts.py`: Utility to sample data for evaluation.
- `prompts/`: System prompts for the LLM Judge.
- `raw_data/`: Input data storage (expects `assignment_data/political_leaning.csv`).
- `preprocessed_data/`: Intermediate cleaned data.
- `results/`: Final output from the LLM Judge.

## Preprocessing Pipeline Details

The `DataPreprocessor` applies the following steps in order:

1.  **Fix Column Typos**: Corrects common errors in raw data headers (e.g., `auhtor_ID` -> `author`).
2.  **Clean Content**: Normalizes text by replacing identifiers (URLs, Emails, etc.) with tokens like `[URL]`, `[EMAIL]`.
3.  **Remove Exact Duplicates**: Removes authors with identical posts to prevent stylometric bias.
4.  **Filter Non-Standard Ratio**: Excludes posts with an excessive ratio of non-alphanumeric characters.
5.  **Remove Messy Posts**: Uses pattern matching to filter out bot messages, ads, and common noise.
6.  **Remove Repetitive Wording**: Sanitizes posts where a single word dominates the content ratio.
7.  **Normalize Post Size**: Enforces length constraints (minimum word count and maximum character count).
8.  **Deduplicate Similar Posts**: Resolves near-duplicates using Jaccard similarity.
    - **Cross-user duplicates**: All instances removed (ambiguous authorship).
    - **Same-user duplicates**: Only the longest version is retained.

## LLM-as-a-Judge

The project features a sophisticated evaluation agent built on **pydantic-ai**.

- **Structured Output**: Returns rich JSON objects containing `judgment`, `classification` (domain and mechanism), and `forensics` (evidence spans).
- **Multi-Model Support**: Easily switch between OpenAI, Google, Groq, Cerebras, OpenRouter, and Hugging Face models via configuration.
- **Resume Capability**: Automatically skips already processed posts if the process is interrupted.
- **Logging & Tracing**: Fully instrumented with Logfire for deep visibility into LLM calls and retry logic.

## License

This project is licensed under the terms of the LICENSE file.
