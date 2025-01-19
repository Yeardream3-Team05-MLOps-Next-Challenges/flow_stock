import os
import logging
from prefect import get_run_logger

def is_prefect_env():
    try:
        get_run_logger()
        return True
    except Exception:
        return False

def setup_logging():
    l_level = getattr(logging, os.getenv('LOGGING_LEVEL'), logging.INFO)
    logging.basicConfig(level=l_level)


def get_logger():
    if is_prefect_env():
        return get_run_logger()
    else:
        return logging.getLogger(__name__)