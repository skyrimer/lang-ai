import logging
import sys


def setup_logging(name: str = __name__, level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a logger with a standard configuration.

    Args:
        name (str): The name of the logger (usually __name__).
        level (int): The logging level (default: logging.INFO).

    Returns:
        logging.Logger: The configured logger instance.
    """
    logger = logging.getLogger(name)

    # prevent adding multiple handlers if setup is called multiple times
    if not logger.handlers:
        logger.setLevel(level)

        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(levelname)s: %(asctime)s  - %(message)s")
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
