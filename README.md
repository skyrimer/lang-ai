# Lang-AI: Language and AI Project

A Python project for stylometric analysis and data preprocessing of Reddit posts.

## Project Overview

This project provides tools for downloading, cleaning, and preprocessing text data (specifically Reddit posts) to prepare it for stylometric analysis. It includes features for text normalization, duplicate removal based on similarity, and handling common data issues.

## Key Features

- **Data Downloading**: Automated download and extraction of raw data from SurfDrive.
- **Text Normalization**: Replaces non-stylometric elements like URLs, emails, user mentions, and IP addresses with specific tokens.
- **Similarity-Based Deduplication**: Identifies and resolves near-duplicate posts using Jaccard similarity and connected components clustering.
- **LLM-as-a-Judge**: Infrastructure to score posts for data leakage and pollution using various LLM providers (OpenAI, Google, Hugging Face).
- **Configurable Pipelines**: Easy-to-define preprocessing steps using Pydantic models.
- **Logging**: Standardized logging for tracking the preprocessing progress.

## Installation

This project uses `uv` for dependency management.

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd lang-ai
   ```

2. **Set up the environment**:
   Create a `.env` file in the root directory and add the required environment variables:
   ```env
   SURFDRIVE_LINK=<your-surfdrive-link>
   SURFDRIVE_PASSWORD=<your-surfdrive-password>
   ```

3. **Install dependencies**:
   ```bash
   make sync
   ```
   Or using `uv` directly:
   ```bash
   uv sync
   ```

## Usage

### 1. Download Raw Data
To download the raw data from SurfDrive:
```bash
uv run src/download_raw_data.py
```

### 2. Run Preprocessing
To run the preprocessing pipeline:
```bash
uv run main.py
```
Alternatively, you can run the preprocessor script directly:
```bash
uv run src/data_preprocessor.py
```
This will:
- Load the raw data.
- Fix column name typos.
- Normalize post content.
- Remove authors with exact duplicate posts.
- Deduplicate similar posts across the dataset.
- Save the cleaned data to `preprocessed_data/preprocessed_data.csv`.

### 3. Run LLM Judge
To run the LLM judge on the preprocessed data:
```bash
uv run run_judge.py --model gpt-4o --provider openai --sample 10
```
Arguments:
- `--input`: Path to input CSV (default: `preprocessed_data/preprocessed_data.csv`).
- `--output`: Path to output CSV (default: `results/judge_results.csv`).
- `--model`: Model name (e.g., `gpt-4o`, `gemini-1.5-pro`, `meta-llama/Llama-3.1-8B-Instruct`).
- `--provider`: Provider (`openai`, `google`, `huggingface`).
- `--prompt`: Path to system prompt file (default: `prompts/gpt_gen_prompt.txt`).
- `--sample`: Number of posts to sample.
- `--batch-size`: Number of parallel requests (default: 5).

## Project Structure

- `src/`: Source code directory.
    - `data_preprocessor.py`: Main preprocessing logic and pipeline definition.
    - `llm_judge.py`: Infrastructure for LLM-based evaluation.
    - `models.py`: Pydantic models for structured data.
    - `similarity_analyser.py`: Text similarity analysis and cluster resolution.
    - `text_normalizer.py`: Regex-based text normalization.
    - `download_raw_data.py`: Utilities for downloading data.
    - `dlm_logger.py`: Logging configuration.
    - `utils.py`: General utility functions.
- `run_judge.py`: CLI script to run the LLM judge.
- `raw_data/`: Directory where raw data is downloaded and extracted.
- `preprocessed_data/`: Directory where the final cleaned data is saved.
- `pyproject.toml`: Project dependencies and configuration.
- `Makefile`: Shortcut commands for project setup and maintenance.

## Preprocessing Steps Details

1. **Fix Typo**: Corrects common typos in the dataset headers (e.g., `auhtor_ID` -> `author`).
2. **Clean Content**: Uses `TextNormalizer` to replace URLs, emails, and other non-stylometric features with tokens like `[URL]`, `[EMAIL]`, etc.
3. **Remove Duplicates**: Identifies authors who have posted identical content multiple times and removes all their posts to avoid biasing the stylometric analysis.
4. **Deduplicate Similar Posts**: Uses character n-gram Jaccard similarity to find near-duplicate posts (e.g., viral content or copypasta). 
    - **Multi-User Clusters**: If the same text is posted by multiple users, all instances are removed as authorship is ambiguous.
    - **Single-User Clusters**: If a user reposts their own content, only the longest version is kept.

## License

This project is licensed under the terms of the LICENSE file.
