from src.logger import setup_logging

def pytest_configure(config):
    setup_logging()