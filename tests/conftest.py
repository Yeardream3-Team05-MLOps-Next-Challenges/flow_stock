import os

def pytest_configure(config):

    os.environ['PREFECT_LOGGING_LEVEL'] = 'INFO'