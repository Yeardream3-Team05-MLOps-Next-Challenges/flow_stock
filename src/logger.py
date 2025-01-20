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
    # 기본 로깅 레벨 설정 
    log_level = os.getenv('LOGGING_LEVEL', 'INFO')
    
    # Python 로깅 설정
    l_level = getattr(logging, log_level, logging.INFO)
    logging.basicConfig(level=l_level)
    
    # Prefect 로깅 레벨도 함께 설정
    os.environ['PREFECT_LOGGING_LEVEL'] = log_level

def get_logger():
    if is_prefect_env():
        return get_run_logger()
    else:
        return logging.getLogger(__name__)