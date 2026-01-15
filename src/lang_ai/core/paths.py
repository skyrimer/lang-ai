"""
Centralized path management for the lang-ai project.

Provides consistent path resolution relative to project root,
eliminating hardcoded path strings scattered across modules.
"""

from pathlib import Path
from typing import Optional


class ProjectPaths:
    """
    Centralized registry of all project paths.

    All paths are resolved relative to the project root, which is determined
    by the location of this file (src/lang_ai/core/paths.py).

    Usage:
        from src.lang_ai.core.paths import ProjectPaths

        # Get directory paths
        raw_dir = ProjectPaths.raw_data()
        results_dir = ProjectPaths.results()

        # Get specific file paths
        input_file = ProjectPaths.POLITICAL_LEANING_CSV
        preprocessed = ProjectPaths.PREPROCESSED_CSV
    """

    @staticmethod
    def root() -> Path:
        """
        Get project root directory.

        Returns:
            Absolute path to project root

        Note:
            Assumes this file is at: project_root/src/lang_ai/core/paths.py
            Therefore root is 4 levels up from this file.
        """
        return Path(__file__).resolve().parent.parent.parent.parent

    @staticmethod
    def raw_data(subpath: str = "") -> Path:
        """
        Get raw data directory or subdirectory.

        Args:
            subpath: Optional subdirectory within raw_data/

        Returns:
            Path to raw_data/ or raw_data/{subpath}
        """
        base = ProjectPaths.root() / "raw_data"
        return base / subpath if subpath else base

    @staticmethod
    def preprocessed_data(subpath: str = "") -> Path:
        """
        Get preprocessed data directory or subdirectory.

        Args:
            subpath: Optional subdirectory within preprocessed_data/

        Returns:
            Path to preprocessed_data/ or preprocessed_data/{subpath}
        """
        base = ProjectPaths.root() / "preprocessed_data"
        return base / subpath if subpath else base

    @staticmethod
    def results(subpath: str = "") -> Path:
        """
        Get results directory or subdirectory.

        Args:
            subpath: Optional subdirectory within results/

        Returns:
            Path to results/ or results/{subpath}
        """
        base = ProjectPaths.root() / "results"
        return base / subpath if subpath else base

    @staticmethod
    def configs(subpath: str = "") -> Path:
        """
        Get configs directory or subdirectory.

        Args:
            subpath: Optional subdirectory within configs/

        Returns:
            Path to configs/ or configs/{subpath}
        """
        base = ProjectPaths.root() / "configs"
        return base / subpath if subpath else base

    @staticmethod
    def prompts(subpath: str = "") -> Path:
        """
        Get prompts directory or subdirectory.

        Args:
            subpath: Optional subdirectory within prompts/

        Returns:
            Path to prompts/ or prompts/{subpath}
        """
        base = ProjectPaths.root() / "prompts"
        return base / subpath if subpath else base

    @staticmethod
    def notebooks(subpath: str = "") -> Path:
        """
        Get notebooks directory or subdirectory.

        Args:
            subpath: Optional subdirectory within notebooks/

        Returns:
            Path to notebooks/ or notebooks/{subpath}
        """
        base = ProjectPaths.root() / "notebooks"
        return base / subpath if subpath else base

    @staticmethod
    def political_leaning_csv() -> Path:
        """Raw political leaning dataset."""
        return ProjectPaths.raw_data("assignment_data/political_leaning.csv")

    @staticmethod
    def preprocessed_csv() -> Path:
        """Default preprocessed data file."""
        return ProjectPaths.preprocessed_data("preprocessed_data.csv")

    @staticmethod
    def deleaked_csv() -> Path:
        """Default deleaked data file."""
        return ProjectPaths.preprocessed_data("deleaked_data.csv")

    @staticmethod
    def sample_csv(sample_name: str = "sample_500") -> Path:
        """
        Get path to a sample CSV file.

        Args:
            sample_name: Name of sample (default: "sample_500")

        Returns:
            Path to preprocessed_data/{sample_name}.csv
        """
        return ProjectPaths.preprocessed_data(f"{sample_name}.csv")

    @staticmethod
    def judge_results_dir(subdir: Optional[str] = None) -> Path:
        """
        Get judge results directory.

        Args:
            subdir: Optional subdirectory within results/

        Returns:
            Path to results/ or results/{subdir}
        """
        if subdir:
            return ProjectPaths.results(subdir)
        return ProjectPaths.results()

    @staticmethod
    def evaluation_results_dir(subdir: str = "deleaking_evaluation") -> Path:
        """
        Get evaluation results directory.

        Args:
            subdir: Subdirectory name (default: "deleaking_evaluation")

        Returns:
            Path to results/{subdir}
        """
        return ProjectPaths.results(subdir)

    @staticmethod
    def ensure_exists(path: Path, is_file: bool = False) -> Path:
        """
        Ensure directory exists (create if needed).

        Args:
            path: Path to directory or file
            is_file: If True, create parent directory instead of path itself

        Returns:
            The input path (for chaining)
        """
        target = path.parent if is_file else path
        target.mkdir(parents=True, exist_ok=True)
        return path