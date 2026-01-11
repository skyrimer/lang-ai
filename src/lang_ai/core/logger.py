"""
Logging utility for the lang-ai project.
"""

import logging
import sys


def setup_logging(name: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a logger with a standard configuration.

    Args:
        name (str | None): The name of the logger (usually __name__).
                           If None, defaults to "lang_ai".
        level (int): The logging level (default: logging.INFO).

    Returns:
        logging.Logger: The configured logger instance.
    """
    if name is None:
        name = "lang_ai"
    logger = logging.getLogger(name)

    # prevent adding multiple handlers if setup is called multiple times
    if not logger.handlers:
        logger.setLevel(level)
        logger.propagate = False
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(levelname)s: %(asctime)s  - %(message)s", datefmt="%d-%m-%Y %H:%M:%S"
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
