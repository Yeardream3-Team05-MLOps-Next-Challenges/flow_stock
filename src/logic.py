import websockets
import json
import requests
import os
import asyncio
from kafka import KafkaProducer
import logging
import datetime
import pytz

from src.logger import get_logger

def setup_kafka_producer_logic():
    """Kafka Producer를 설정하는 순수 함수."""
    logger = get_logger()

    try:
        producer = KafkaProducer(
            acks=0,
            compression_type='gzip',
            bootstrap_servers=os.getenv('KAFKA_URL', 'default_url'),
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(2,)
        )
        logger.debug('Kafka Producer 설정 완료')
        return producer
    except Exception as e:
        logger.error(f"Kafka Producer 설정 중 오류 발생: {e}")
        raise

def get_config_logic():
    """설정 정보를 가져오는 순수 함수."""
    logger = get_logger()
    logger.debug('get_config 호출됨')
    return {
        "CODE_LIST": [
            ['1', 'H0STASP0', '005930'],
            ['1', 'H0STASP0', '051910'],
            ['1', 'H0STASP0', '000660'],
        ],
        "WEBSOCKET_URL": 'ws://ops.koreainvestment.com:31000',
        "API_URL": 'https://openapivts.koreainvestment.com:29443',
        "KAFKA_TOPIC": 'tt_tick',
        "appkey": os.getenv('APP_KEY', 'default_key'),
        "appsecret": os.getenv('APP_SECRET', 'default_secret'),
        "htsid": os.getenv('HTS_ID', 'default_id'),
    }

def get_approval_logic(key: str, secret: str):
    """인증 키를 가져오는 순수 함수."""
    logger = get_logger()
    logger.debug('get_approval 호출됨')

    url = 'https://openapivts.koreainvestment.com:29443'
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": key, "secretkey": secret}
    PATH = "oauth2/Approval"
    URL = f"{url}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    approval_key = res.json()["approval_key"]
    return approval_key

async def send_to_kafka_logic(data: dict, producer):
    """Kafka로 데이터를 전송하는 순수 함수."""
    logger = get_logger()
    
    if producer is None:
        logger.error("Kafka Producer가 설정되지 않았습니다.")
        raise ValueError("Kafka Producer가 설정되지 않았습니다.")
    
    logger.debug('Kafka로 전송 시작')
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    data['날짜'] = current_date
    producer.send(get_config_logic()["KAFKA_TOPIC"], value=data)
    logger.debug('Kafka로 데이터 전송 완료')

async def stockhoka_logic(data: str, producer):
    """주식 호가 데이터를 처리하는 순수 함수."""
    logger = get_logger()
    logger.debug('stockhoka 처리 시작')
    recvvalue = data.split('^')
    await send_to_kafka_logic({
        "종목코드": recvvalue[0],
        "현재가": recvvalue[3],
        "현재시간": recvvalue[1],
    }, producer)
    logger.debug('stockhoka 처리 완료')

async def connect_logic(
    stop_event: asyncio.Event,
    websocket_url: str,
    code_list
):
    """WebSocket에 연결하는 순수 함수."""
    logger = get_logger()
    try:
        logger.debug('WebSocket 연결 시작')
        config = get_config_logic()
        g_approval_key = get_approval_logic(config["appkey"], config["appsecret"])
        url = websocket_url

        senddata_list = [json.dumps({
            "header": {
                "approval_key": g_approval_key,
                "custtype": "P",
                "tr_type": i,
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": j,
                    "tr_key": k
                }
            }
        }) for i, j, k in code_list]

        async with websockets.connect(url, ping_interval=None) as ws:
            for senddata in senddata_list:
                await ws.send(senddata)
                await asyncio.sleep(0.5)

            while not stop_event.is_set():
                try:
                    data = await asyncio.wait_for(ws.recv(), timeout=10)
                    logger.debug(f"WebSocket 데이터 수신: {data}")

                    if data.startswith("{"):
                        json_data = json.loads(data)
                        if json_data["header"]["tr_id"] == "PINGPONG":
                            logger.debug("PINGPONG 메시지 수신, 응답 전송")
                            await ws.send(data)
                    else:
                        recvstr = data.split('|')
                        if recvstr[1] == "H0STASP0":
                            await stockhoka_logic(recvstr[3], producer=None)
                except asyncio.TimeoutError:
                    logger.debug("Timeout 발생, 연결 유지 확인 중")
                    continue
                except websockets.ConnectionClosed:
                    logger.warning("웹소켓 연결 종료됨")
                    break
    except Exception as e:
        logger.error(f"WebSocket 연결 오류: {e}")
    finally:
        logger.info('WebSocket 연결 종료')

async def check_time_logic(stop_event: asyncio.Event):
    """정해진 시간이 되면 종료하는 순수 함수."""
    logger = get_logger()
    while True:
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.datetime.now(kst)
        if now.hour >= 20:  # 오후 8시 이후
            logger.info("8PM이 되어 종료를 시작합니다.")
            stop_event.set()
            raise SystemExit("8PM 종료 완료")
        await asyncio.sleep(60)