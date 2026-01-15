"""
Core utility functions for the lang-ai project.
"""

import os


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
