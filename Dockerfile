# Dockerfile
FROM prefecthq/prefect:2.18.3-python3.10

ARG LOGGING_LEVEL
ARG APP_KEY
ARG APP_SECRET
ARG HTS_ID
ARG KAFKA_URL

ENV PREFECT_LOGGING_LEVEL=${LOGGING_LEVEL}
ENV LOGGING_LEVEL=${LOGGING_LEVEL}
ENV APP_KEY=${APP_KEY}
ENV APP_SECRET=${APP_SECRET}
ENV HTS_ID=${HTS_ID}
ENV KAFKA_URL=${KAFKA_URL}

# 작업 디렉토리 설정
COPY requirements.txt .

# 필요한 파이썬 패키지 설치
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

# Prefect 에이전트 실행
CMD ["python", "./flow.py"]