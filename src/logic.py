import os
import json
import requests
import websockets
import asyncio
import datetime
from typing import Optional, Dict
from kafka import KafkaProducer
import pytz

from src.logger import get_logger

async def process_market_data(data: str, producer: KafkaProducer, topic: str):
    """시장 데이터 처리 로직"""
    logger = get_logger()
    try:
        parsed_data = parse_data(data)
        enriched_data = add_timestamp(parsed_data)
        await send_to_kafka(enriched_data, producer, topic)
    except Exception as e:
        logger.error(f"데이터 처리 실패: {e}")

def parse_data(raw_data: str) -> Dict:
    """웹소켓 데이터 파싱"""
    parts = raw_data.split('|')
    return {
        "종목코드": parts[0],
        "현재가": parts[3],
        "타임스탬프": parts[1]
    }

def add_timestamp(data: Dict) -> Dict:
    """타임스탬프 추가"""
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    return {**data, "수신시간": now.isoformat()}

async def send_to_kafka(data: Dict, producer: KafkaProducer, topic: str):
    """Kafka 전송 로직"""
    try:
        producer.send(topic, value=data)
    except Exception as e:
        get_logger().error(f"Kafka 전송 실패: {e}")
        raise

def load_config() -> Dict:
    """환경 설정 로드"""
    return {
        "CODE_LIST": [
            ['1', 'H0STASP0', '005930'],
            ['1', 'H0STASP0', '051910'],
            ['1', 'H0STASP0', '000660'],
        ],
        "WEBSOCKET_URL": os.getenv("WEBSOCKET_URL", "ws://ops.koreainvestment.com:31000"),
        "API_URL": os.getenv("API_URL", "https://openapivts.koreainvestment.com:29443"),
        "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "tt_tick"),
        "APP_KEY": os.getenv("APP_KEY"),
        "APP_SECRET": os.getenv("APP_SECRET")
    }

def get_api_approval(api_url: str, appkey: str, secret: str) -> str:
    """API 인증 키 획득"""
    response = requests.post(
        f"{api_url}/oauth2/Approval",
        headers={"content-type": "application/json"},
        json={"grant_type": "client_credentials", "appkey": appkey, "secretkey": secret}
    )
    response.raise_for_status()
    return response.json()["approval_key"]

async def connect_websocket(
    config: Dict,
    producer: KafkaProducer,
    auth_key: str,
    stop_event: asyncio.Event
):
    """웹소켓 연결 관리"""
    logger = get_logger()
    try:
        async with websockets.connect(config["WEBSOCKET_URL"]) as ws:
            await initialize_connection(ws, config["CODE_LIST"], auth_key)
            await handle_messages(ws, producer, config["KAFKA_TOPIC"], stop_event)
    except Exception as e:
        logger.error(f"웹소켓 연결 오류: {e}")
        stop_event.set()

async def initialize_connection(ws, code_list, auth_key):
    """웹소켓 초기 연결 설정"""
    for tr_type, tr_id, tr_key in code_list:
        await ws.send(json.dumps({
            "header": {
                "approval_key": auth_key,
                "custtype": "P",
                "tr_type": tr_type,
                "content-type": "utf-8"
            },
            "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}}
        }))
        await asyncio.sleep(0.5)

async def handle_messages(ws, producer, topic, stop_event):
    """메시지 처리 핸들러"""
    while not stop_event.is_set():
        try:
            data = await asyncio.wait_for(ws.recv(), timeout=15)
            if data.startswith("{"):
                await handle_control_message(data, ws)
            else:
                await process_market_data(data, producer, topic)
        except asyncio.TimeoutError:
            continue
        except websockets.ConnectionClosed:
            break

async def handle_control_message(data: str, ws):
    """제어 메시지 처리"""
    logger = get_logger()
    message = json.loads(data)
    if message.get("header", {}).get("tr_id") == "PINGPONG":
        logger.debug("PINGPONG 응답")
        await ws.send(data)

async def monitor_shutdown(stop_event: asyncio.Event):
    """종료 시간 모니터링"""
    logger = get_logger()
    while not stop_event.is_set():
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.datetime.now(kst)
        if now.hour >= 20:
            logger.info("20:00 종료 트리거")
            stop_event.set()
        await asyncio.sleep(60)