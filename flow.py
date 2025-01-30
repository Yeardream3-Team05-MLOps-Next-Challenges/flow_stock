from prefect import task, flow, get_run_logger
from typing import Optional
import asyncio
from kafka import KafkaProducer
import os
import json

from src.logger import get_logger, setup_logging
from src.logic import (
    load_config,
    get_api_approval,
    connect_websocket,
    process_market_data,
    monitor_shutdown
)

@task(name="Initialize Kafka Producer", retries=2, retry_delay_seconds=5)
def init_kafka_producer() -> Optional[KafkaProducer]:
    """Kafka Producer 초기화 태스크"""
    logger = get_run_logger()
    try:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_URL", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=0,
            compression_type='gzip',
            api_version=(2,)
        )
        logger.debug("Kafka Producer 초기화 완료")
        return producer
    except Exception as e:
        logger.error(f"Kafka 초기화 실패: {e}")
        raise

@task(name="Load Configuration")
def load_config_task() -> dict:
    """환경 설정 로드 태스크"""
    return load_config()

@task(name="Get API Authorization")
def get_auth_task(config: dict) -> str:
    """API 인증 키 획득 태스크"""
    logger = get_run_logger()
    try:
        return get_api_approval(
            config["API_URL"],
            config["APP_KEY"],
            config["APP_SECRET"]
        )
    except Exception as e:
        logger.critical("인증 키 획득 실패: %s", str(e))
        raise

@flow(
    name="Stock Data Pipeline",
    timeout_seconds=46800, 
    description="주식 시장 데이터 수집 파이프라인 (08:00 ~ 20:00)"
)
async def stock_data_pipeline():
    """주식 데이터 처리 메인 플로우"""
    logger = get_run_logger()
    stop_event = asyncio.Event()
    
    # 의존성 주입
    config = load_config_task()
    producer = init_kafka_producer()
    auth_key = get_auth_task(config)
    
    try:
        await asyncio.gather(
            connect_websocket(config, producer, auth_key, stop_event),
            monitor_shutdown(stop_event)
        )
    except Exception as e:
        logger.error(f"플로우 실행 중 오류 발생: {e}")
        stop_event.set()
    finally:
        await graceful_shutdown(producer)

@task(name="Graceful Shutdown")
async def graceful_shutdown(producer: KafkaProducer):
    """리소스 정리 태스크"""
    logger = get_run_logger()
    try:
        logger.info("리소스 정리 시작")
        producer.flush(timeout=5)
        producer.close()
        logger.debug("Kafka Producer 종료 완료")
    except Exception as e:
        logger.warning(f"리소스 정리 중 오류: {e}")

if __name__ == "__main__":
    asyncio.run(stock_data_pipeline())