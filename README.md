# Language and AI Project

A Python project for stylometric analysis, data preprocessing, and LLM-based evaluation of Reddit posts.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Pipeline Guide](#pipeline-guide)
   - [Pipeline 1: Judge Pipeline](#pipeline-1-judge-pipeline)
   - [Pipeline 2: Model Evaluation Pipeline](#pipeline-2-model-evaluation-pipeline)
6. [Project Structure](#project-structure)
7. [Preprocessing Pipeline Details](#preprocessing-pipeline-details)
8. [LLM-as-a-Judge](#llm-as-a-judge)
9. [Advanced Usage](#advanced-usage)

---

## Project Overview

This project provides a robust pipeline for cleaning and preprocessing text data (specifically Reddit posts) to prepare it for stylometric analysis. It includes advanced features for text normalization, multi-stage filtering, similarity-based deduplication, and an LLM-as-a-Judge infrastructure for data leakage detection.

---

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

---

## Installation

This project uses `uv` for lightning-fast dependency management.

### 1. Clone the repository
```bash
git clone <repository-url>
cd lang-ai
```

### 2. Set up the environment
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

### 3. Prepare the data
Place the `political_leaning.csv` dataset in the `raw_data/assignment_data/` directory:
```bash
mkdir -p raw_data/assignment_data
# Move your political_leaning.csv there
```

### 4. Install dependencies
```bash
make sync
```

### 5. Configure Observability (Optional)
```bash
make logfire-auth
```

---

## Quick Start

### Judge Pipeline (LLM-based Quality Assessment)
```bash
# Full integrated pipeline: Sampling → Multi-judge → Analysis → Best model on full dataset
python run_judge_pipeline.py
```

### Model Evaluation Pipeline (Deleaking Methodology Validation)
```bash
# Full pipeline: Preprocessing → Deleakage → Evaluation
python run_model_pipeline.py
```

---

## Pipeline Guide

### Pipeline Overview

#### **Pipeline 1: Judge Pipeline**
**Purpose:** Creates samples, runs LLM judges, analyzes results, and runs best model on full dataset

**Steps:**
1. (Optional) Create stratified sample from dataset
2. Run multiple LLM judges on sample for comparison
3. Analyze judge results (Dawid-Skene, Krippendorff's Alpha, Bootstrap)
4. Identify best performing judge
5. Run best judge on full dataset

**Outputs:**
- Multi-judge results: `results/judge_pipeline/judge_results_{model}_{sample}.csv`
- Analysis report: `results/judge_pipeline/judge_analysis_report_{sample}.json`
- Disagreement samples: `results/judge_pipeline/disagreement_samples_{sample}.csv`
- Best model results: `results/judge_pipeline/judge_results_{best_model}_{full_dataset}.csv`

#### **Pipeline 2: Model Evaluation Pipeline**
**Purpose:** Full data processing and model evaluation with baseline vs sanitized comparison

**Steps:**
1. Preprocessing: Remove pollution (bots, ads, AI-generated content)
2. Deleakage: Remove political leakage patterns (nicknames, pejoratives, slang)
3. Evaluation: Compare baseline (preprocessed) vs sanitized (deleaked) using ML models

**Outputs:**
- Evaluation results (JSON)
- Comparison reports (JSON, CSV)
- Feature analysis (JSON)

---

## Pipeline 1: Judge Pipeline

### Configuration File

Edit `configs/judge_config.yaml`:

```yaml
sampling:
  enabled: true
  input: "preprocessed_data/preprocessed_data.csv"
  output: "preprocessed_data/sample_500.csv"
  sample_size: 500
  random_seed: 42
  stratify_column: "political_label"  # or null for random sampling

# Multi-judge configuration (models for comparison)
multi_judge:
  system_prompt: "prompts/llmaj_prompt_by_grazie"
  results_dir: "results/judge_pipeline"
  models:
    - "openrouter:x-ai/grok-4.1-fast"
    - "openrouter:google/gemini-3-flash-preview"
    - "openrouter:xiaomi/mimo-v2-flash:free"
    - "openrouter:openai/gpt-oss-120b"
    - "openrouter:qwen/qwen3-235b-a22b-2507"

# Analysis configuration
analysis:
  enabled: true
  disagreement_n: 25
  golden_labels_path: null  # Optional path to golden labels
  bootstrap_iterations: 100

# Best model configuration
best_model:
  model: "google:gemini-3-flash-preview"  # Best model from analysis
  run_on_full_dataset: true
  full_dataset_path: null  # Uses sampling.input if null
```

### Basic Usage

#### 1. Full integrated pipeline (recommended)
```bash
# Sample → Multi-judge → Analysis → Best model on full dataset
python run_judge_pipeline.py
```

**What happens:**
1. Creates 500-sample stratified by political_label
2. Runs 5 models on the sample
3. Analyzes results and identifies best model (e.g., gemini)
4. Runs best model on full dataset

#### 2. Run without sampling
```bash
# Multi-judge on full dataset → Analysis → Best model already done
python run_judge_pipeline.py --no-sample
```

#### 3. Skip analysis step
```bash
# Just multi-judge, no analysis or best model run
python run_judge_pipeline.py --skip-analysis --skip-best-model
```

#### 4. Skip best model full dataset run
```bash
# Sample → Multi-judge → Analysis (stop here)
python run_judge_pipeline.py --skip-best-model
```

#### 5. Manually specify best model
```bash
# Override analysis result with manual best model selection
python run_judge_pipeline.py --best-model google:gemini-3-flash-preview
```

#### 6. Override sample size
```bash
python run_judge_pipeline.py --sample-size 1000
```

#### 7. Override models for multi-judge
```bash
python run_judge_pipeline.py --models model1 model2 model3
```

#### 8. Custom full dataset path
```bash
python run_judge_pipeline.py --full-dataset preprocessed_data/custom_full.csv
```

### Alternative: Using Hydra Config Overrides

```bash
# Override via Hydra syntax
python run_judge_pipeline.py \
  sampling.sample_size=1000 \
  sampling.enabled=false \
  multi_judge.models='[google:gemini-3-flash-preview]'
```

### Output

Results saved to `results/judge_pipeline/`:

**Multi-judge results (Step 2):**
- `judge_results_openrouter_x-ai_grok-4.1-fast_sample_500.csv`
- `judge_results_openrouter_google_gemini-3-flash-preview_sample_500.csv`
- `judge_results_openrouter_xiaomi_mimo-v2-flash_free_sample_500.csv`
- `judge_results_openrouter_openai_gpt-oss-120b_sample_500.csv`
- `judge_results_openrouter_qwen_qwen3-235b-a22b-2507_sample_500.csv`

**Analysis results (Step 3):**
- `judge_analysis_report_sample_500.json` - Analysis summary with best model
- `disagreement_samples_sample_500.csv` - 25 samples with most judge disagreement

**Best model results (Step 4):**
- `judge_results_google_gemini-3-flash-preview_preprocessed_data.csv` - Full dataset results

**Each judge CSV contains:**
- Original post data (post, author, political_label, etc.)
- Judge verdict (`judgment.is_leaky`: true/false)
- Confidence level (`judgment.confidence`: HIGH/MEDIUM/LOW)
- Severity (`judgment.severity_score`: CRITICAL/MODERATE/LOW)
- Leakage classification (`classification.leakage_domain`, `classification.specific_mechanism`)
- Evidence spans (`forensics.evidence_spans`: list of quoted text)
- Evidence location (`forensics.evidence_location`: Beginning/Middle/End/Scattered)

**Analysis report JSON format:**
```json
{
  "dataset_name": "sample_500",
  "num_judges": 5,
  "krippendorff_alpha": 0.7234,
  "best_model": "google:gemini-3-flash-preview",
  "bootstrap_iterations": 100,
  "has_golden_labels": false
}
```

**Console output includes:**
- Krippendorff's Alpha (inter-rater reliability)
- Bootstrap Stability Analysis table (DS Reliability, Brier Score, Hallucinations, etc.)
- Best model identification with highest DS Reliability score

---

## Pipeline 2: Model Evaluation Pipeline

### Quick Start

#### 1. Run full pipeline (preprocessing → deleakage → evaluation)
```bash
python run_model_pipeline.py
```

This will:
- Load `raw_data/assignment_data/political_leaning.csv`
- Preprocess → `preprocessed_data/preprocessed_data.csv` (baseline)
- Deleakage → `preprocessed_data/deleaked_data.csv` (sanitized)
- Evaluate → `results/deleaking_evaluation/`

#### 2. Skip preprocessing (use existing preprocessed data)
```bash
python run_model_pipeline.py --skip-preprocessing
```

Useful when you've already preprocessed the data and only want to re-run deleakage + evaluation.

#### 3. Skip deleakage (use existing deleaked data)
```bash
python run_model_pipeline.py --skip-deleakage
```

Useful when you want to re-run only the evaluation with different settings.

#### 4. Only run evaluation (skip preprocessing and deleakage)
```bash
python run_model_pipeline.py --only-evaluation
```

Fastest option when both preprocessed and deleaked files already exist.

#### 5. Use custom paths
```bash
python run_model_pipeline.py \
  --raw-data raw_data/custom.csv \
  --preprocessed preprocessed_data/custom_preprocessed.csv \
  --deleaked preprocessed_data/custom_deleaked.csv \
  --results results/custom_evaluation
```

#### 6. Run multiple experiments
```bash
# Experiment 1: Default settings
python run_model_pipeline.py --results results/experiment_001

# Experiment 2: Re-evaluate with different config
python run_model_pipeline.py --only-evaluation --results results/experiment_002

# Experiment 3: Different raw data
python run_model_pipeline.py --raw-data raw_data/alternative.csv --results results/experiment_003
```

### Output

Results saved to `results/deleaking_evaluation/` (or custom `--results` path):

```
results/deleaking_evaluation/
├── baseline_evaluation.json          # Baseline corpus evaluation
├── sanitized_evaluation.json         # Sanitized corpus evaluation
├── comparison_report.json            # Side-by-side comparison
├── comparison_summary.csv            # Quick summary table
└── feature_analysis/
    ├── baseline_top_features.json    # Top 50 features per class (baseline)
    └── sanitized_top_features.json   # Top 50 features per class (sanitized)
```

### Key Metrics in Output

**Cross-Validation Results:**
- **Performance Stability**: Macro F1 scores with mean ± standard deviation across folds
- **Feature Stability Matrix**: Mean Jaccard overlap ± standard deviation for top-N features aggregated across all classes
- **Leakage Quantification**: Jaccard overlap between baseline and deleaked consensus features per class at various top-N thresholds

**Output Files:**
- `comparative_feature_stability.csv`: Feature consistency across CV folds (baseline vs deleaked)
- `cv_leakage_quantification.csv`: Per-class feature overlap analysis

**Interpretation:**
- **Lower feature overlap** between baseline and deleaked = Successful leakage removal
- **Consistent F1 across folds** = Stable model performance
- **High feature stability** = Reliable feature selection

---

## Project Structure

- `src/lang_ai/`: Core source code.
    - `data/`: Data handling and transformation.
        - `preprocessor.py`: Orchestration of the cleaning pipeline.
        - `normalizer.py`: Text cleaning and tokenization logic.
        - `filters.py`: Regex patterns for identifying "messy" content.
        - `leakage_filter.py`: Political leakage pattern filtering.
        - `deleakage_pipeline.py`: Deleakage workflow orchestration.
        - `base_filter.py`: Abstract base class for text filters.
    - `judge/`: LLM evaluation logic.
        - `agent.py`: `pydantic-ai` agent implementation and pipeline.
        - `judge_analysis.py`: Evaluation metrics and agreement analysis.
        - `labeling_app.py`: Streamlit app for manual labeling.
        - `sampling.py`: Stratified/random sampling functionality.
    - `evaluation/`: Model evaluation and comparison.
        - `stability_evaluation.py`: Comparative evaluation using Stratified Group K-Fold CV.
    - `analysis/`: Algorithmic data analysis.
        - `similarity.py`: Jaccard similarity and clustering utilities for deduplication.
    - `core/`: Shared infrastructure.
        - `models.py`: Structured data definitions (Pydantic).
        - `logger.py`: Logging setup.
        - `utils.py`: Environment and path utilities.
        - `data_io.py`: Unified CSV/JSON I/O operations.
        - `paths.py`: Centralized path management.
        - `base_pipeline.py`: Abstract base class for pipelines.
- `run_judge_pipeline.py`: Judge pipeline orchestrator (sampling → multi-judge → analysis → best model).
- `run_model_pipeline.py`: Model pipeline orchestrator (preprocessing → deleakage → evaluation).
- `prompts/`: System prompts for the LLM Judge.
- `configs/`: Configuration files.
    - `judge_config.yaml`: Judge pipeline configuration.
    - `model_config.yaml`: Model preprocessing/deleaking/evaluation configuration.
- `raw_data/`: Input data storage (expects `assignment_data/political_leaning.csv`).
- `preprocessed_data/`: Intermediate cleaned data.
- `results/`: Final output from pipelines.

---

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

---

## LLM-as-a-Judge

The project features a sophisticated evaluation agent built on **pydantic-ai**.

- **Structured Output**: Returns rich JSON objects containing `judgment`, `classification` (domain and mechanism), and `forensics` (evidence spans).
- **Multi-Model Support**: Easily switch between OpenAI, Google, Groq, Cerebras, OpenRouter, and Hugging Face models via configuration.
- **Resume Capability**: Automatically skips already processed posts if the process is interrupted.
- **Logging & Tracing**: Fully instrumented with Logfire for deep visibility into LLM calls and retry logic.

---

## Advanced Usage

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

The project uses **Hydra** for configuration management. All parameters are defined in configuration files and can be overridden via command-line arguments.

### Standalone Pipeline Components

You can also run individual pipeline steps standalone:

#### Preprocessing Only
```bash
python -m src.lang_ai.data.preprocessor
```

Or programmatically:
```python
from src.lang_ai.data.preprocessor import preprocess_data
from src.lang_ai.core.paths import ProjectPaths

preprocess_data(
    data_url=ProjectPaths.political_leaning_csv(),
    output_path=ProjectPaths.preprocessed_csv()
)
```

#### Deleakage Only
```bash
python -m src.lang_ai.data.deleakage_pipeline
```

Or programmatically:
```python
from src.lang_ai.data.deleakage_pipeline import deleakage_pipeline
from src.lang_ai.core.paths import ProjectPaths

deleakage_pipeline(
    input_path=ProjectPaths.preprocessed_csv(),
    output_path=ProjectPaths.deleaked_csv()
)
```

#### Evaluation Only
Programmatically:
```python
from pathlib import Path
from src.lang_ai.evaluation.stability_evaluation import main as evaluation

cfg = {} #your config overrides here
evaluation(cfg)
```

---

## Quick Reference

### Pipeline Comparison

| Feature | Judge Pipeline | Model Evaluation Pipeline |
|---------|---------------|---------------------------|
| **Input** | Raw or preprocessed CSV | Raw CSV |
| **Processing** | Sampling (optional) | Preprocessing → Deleakage |
| **Analysis** | LLM-based quality assessment | ML-based stylometric analysis |
| **Output** | Judge verdicts per post | Aggregate metrics + comparisons |
| **Speed** | Depends on LLM API | Fast (local computation) |
| **Use Case** | Quality control, labeling | Methodology validation |

### Command Quick Reference

```bash
# Judge Pipeline
python run_judge_pipeline.py                    # Full run with sampling
python run_judge_pipeline.py --no-sample        # Skip sampling
python run_judge_pipeline.py --sample-size 1000 # Custom sample size

# Model Pipeline
python run_model_pipeline.py                    # Full pipeline
python run_model_pipeline.py --skip-preprocessing  # Skip step 1
python run_model_pipeline.py --skip-deleakage     # Skip step 2
python run_model_pipeline.py --only-evaluation    # Only step 3
```

---

## Getting Help

- **Documentation:** See individual module docstrings for detailed API docs
- **Examples:** Check `notebooks/` for Jupyter notebook examples
- **Configuration:** Review `configs/` for all available settings
- **Issues:** Report bugs at project GitHub issues page

---