# Dockerfile
FROM prefecthq/prefect:2.18.3-python3.10

# 작업 디렉토리 설정
COPY requirements.txt .

# 필요한 파이썬 패키지 설치
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

ENV APP_KEY=api_key
ENV APP_SECRET=fluentd_url
ENV HTS_ID=INFO
ENV SERVER_HOST=host


COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

# Prefect 에이전트 실행
CMD ["python", "./flow.py"]