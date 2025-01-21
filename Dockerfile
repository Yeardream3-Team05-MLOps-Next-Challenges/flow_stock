# Dockerfile
FROM prefecthq/prefect:2.18.3-python3.10

ARG LOGGING_LEVEL
ARG APP_KEY
ARG APP_SECRET
ARG HTS_ID
ARG KAFKA_URL
ARG VERSION

ENV VERSION=$VERSION
ENV LOGGING_LEVEL=${LOGGING_LEVEL}
ENV PREFECT_LOGGING_LEVEL=${LOGGING_LEVEL}
ENV APP_KEY=${APP_KEY}
ENV APP_SECRET=${APP_SECRET}
ENV HTS_ID=${HTS_ID}
ENV KAFKA_URL=${KAFKA_URL}

COPY pyproject.toml poetry.lock* ./

RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

# Prefect 에이전트 실행
CMD ["python", "./flow.py"]