import logging
import sys

_logger_name = "kickdown"


def default_logger() -> logging.Logger:
    logger = logging.getLogger(_logger_name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
