import os
from pathlib import Path


def get_env_var(key: str) -> str:
    """
    Retrieves an environment variable by its key.

    Args:
        key (str): The name of the environment variable.

    Returns:
        str: The value of the environment variable.

    Raises:
        KeyError: If the environment variable is not set.
    """
    try:
        return os.environ[key]
    except KeyError:
        raise KeyError(
            f"Environment variable {key} is not set. Check the environment variables in .env file."
        )


def get_project_root() -> Path:
    """
    Returns the root directory of the project.

    Returns:
        Path: The absolute path to the project root.
    """
    return Path(os.getcwd())
