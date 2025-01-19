from prefect import task, flow
import asyncio
from typing import Optional, Dict, Any
from dataclasses import dataclass
from contextlib import asynccontextmanager

from src.logger import get_logger, setup_logging
from src.logic import (
    setup_kafka_producer_logic,
    get_config_logic,
    get_approval_logic,
    send_to_kafka_logic,
    stockhoka_logic,
    connect_logic,
    check_time_logic
)

@dataclass
class AppState:
    """애플리케이션 상태를 관리하는 클래스"""
    producer: Optional[Any] = None
    config: Optional[Dict] = None

    def cleanup(self):
        """리소스 정리를 위한 메서드"""
        if self.producer is not None:
            self.producer.close()
            self.producer = None


# 전역 상태 관리
app_state = AppState()

@asynccontextmanager
async def managed_resources():
    """리소스 생명주기 관리를 위한 컨텍스트 매니저"""
    logger = get_logger()
    try:
        yield
    finally:
        app_state.cleanup()
        logger.info("Resources cleaned up.")

@task(name="Setup Kafka Producer")
def setup_kafka_producer() -> None:
    """Kafka Producer를 설정하는 Prefect 태스크"""
    app_state.producer = setup_kafka_producer_logic()

@task(name="Get Config")
def get_config() -> Dict:
    """설정 정보를 가져오는 Prefect 태스크"""
    app_state.config = get_config_logic()
    return app_state.config

@task(name="Get Approval")
def get_approval(key: str, secret: str) -> str:
    """인증 키를 가져오는 Prefect 태스크"""
    return get_approval_logic(key, secret)

@task(name="Send to Kafka")
async def send_to_kafka(data: Dict) -> None:
    """Kafka로 데이터를 전송하는 Prefect 태스크"""
    await send_to_kafka_logic(data, app_state.producer)

@task(name="Process Stock Hoka")
async def stockhoka(data: str) -> None:
    """주식 호가 데이터를 처리하는 Prefect 태스크"""
    await stockhoka_logic(data, app_state.producer)

@task(name="Connect to WebSocket")
async def run_connect(stop_event: asyncio.Event, websocket_url: str = None, code_list = None) -> None:
    """WebSocket에 연결하는 Prefect 태스크"""
    config = get_config()
    await connect_logic(stop_event, websocket_url or config["WEBSOCKET_URL"], code_list or config["CODE_LIST"])

@task(name="Check Time")
async def check_time(stop_event: asyncio.Event) -> None:
    """정해진 시간이 되면 종료하는 Prefect 태스크"""
    await check_time_logic(stop_event)

@flow
def hun_fetch_and_send_stock_flow() -> None:
    """전체 데이터 처리 플로우를 정의하는 Prefect 플로우"""
    setup_logging()
    logger = get_logger()
    
    async def async_flow():
        stop_event = asyncio.Event()
        
        async with managed_resources():
            try:
                setup_kafka_producer()
                config = get_config()
                
                connect_task = asyncio.create_task(
                    run_connect.fn(stop_event)
                )
                
                await check_time.fn(stop_event)
                
            except SystemExit as e:
                logger.info(f"종료 이벤트 발생: {e}")
            finally:
                stop_event.set()
                connect_task.cancel()
                try:
                    await connect_task
                except asyncio.CancelledError:
                    logger.info("Connect Task 강제 취소됨")
    
    asyncio.run(async_flow())

if __name__ == "__main__":
    hun_fetch_and_send_stock_flow()