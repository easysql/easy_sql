import codecs
import logging
import sys
from datetime import datetime
from functools import wraps
from typing import Callable

LOG_LEVEL = logging.DEBUG


def _config_logger():
    logger = logging.getLogger("simple_logger")
    logger.setLevel(LOG_LEVEL)
    python_version = sys.version_info
    if python_version.major == 3 and python_version.minor == 6:
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())  # type: ignore
    elif hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(
        "[%(asctime)s][%(processName)s:%(threadName)s][%(levelname)s][%(module)s.%(funcName)s:%(lineno)d] %(message)s"
    )
    handler.setFormatter(formatter)

    for existing_handler in logger.handlers:
        logger.removeHandler(existing_handler)
    logger.addHandler(handler)

    return logger


def log_time(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        try:
            return func(*args, **kwargs)
        finally:
            end_time = datetime.now()
            logger.debug("function {} took {}s".format(func.__name__, (end_time - start_time).total_seconds()))

    return wrapper


logger = _config_logger()
