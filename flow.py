import websockets
import json
import requests
import os
import asyncio
from kafka import KafkaProducer
import logging
import datetime
import pytz
from prefect import task, flow, get_run_logger

# Prefect 환경 여부를 확인하는 함수
def is_prefect_env():
    try:
        get_run_logger()
        return True
    except Exception:
        return False

# 로깅 설정
if not is_prefect_env():
    l_level = getattr(logging, os.getenv('LOGGING_LEVEL'), logging.INFO)
    logging.basicConfig(level=l_level)

def get_logger():
    if is_prefect_env():
        return get_run_logger()
    else:
        return logging.getLogger(__name__)

def setup_kafka_producer():
    global producer
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
    except Exception as e:
        logger.error(f"Kafka Producer 설정 중 오류 발생: {e}")
        raise

def get_config():
    logger = get_logger()
    logger.debug('get_config 호출됨')
    return {
        "appkey": os.getenv('APP_KEY', 'default_key'),
        "appsecret": os.getenv('APP_SECRET', 'default_secret'),
        "htsid": os.getenv('HTS_ID', 'default_id'),
        "kafka_topic": "tt_tick",
    }

def get_approval(key, secret):
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

async def send_to_kafka(data):
    global producer
    logger = get_logger()

    if producer is None:
        setup_kafka_producer()
    logger.debug('Kafka로 전송 시작')
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    data['날짜'] = current_date
    producer.send(get_config()["kafka_topic"], value=data)
    logger.debug('Kafka로 데이터 전송 완료')

async def stockhoka(data):
    logger = get_logger()

    logger.debug('stockhoka 처리 시작')
    recvvalue = data.split('^')
    await send_to_kafka({
        "종목코드": recvvalue[0],
        "현재가": recvvalue[3],
        "현재시간": recvvalue[1],
    })
    logger.debug('stockhoka 처리 완료')

async def connect(stop_event: asyncio.Event):
    logger = get_logger()
    try:
        logger.debug('WebSocket 연결 시작')
        config = get_config()
        g_approval_key = get_approval(config["appkey"], config["appsecret"])
        url = 'ws://ops.koreainvestment.com:31000'

        code_list = [
            ['1', 'H0STASP0', '005930'],
            ['1', 'H0STASP0', '051910'],
            ['1', 'H0STASP0', '000660'],
        ]
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
                            await stockhoka(recvstr[3])
                except asyncio.TimeoutError:
                    logger.debug("Timeout 발생, 연결 유지 확인 중")
                    continue
                except websockets.ConnectionClosed:
                    logger.warning("웹소켓 연결 종료됨")
                    break
    except Exception as e:
        logger.error(f"WebSocket 연결 오류: {e}")
    finally:
        if producer:
            producer.close()
        logger.info('WebSocket 연결 종료')

@task
async def run_connect(stop_event: asyncio.Event):
    await connect(stop_event)

@task
async def check_time(stop_event: asyncio.Event):
    logger = get_logger()
    while True:
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.datetime.now(kst)
        if now.hour >= 20:  # 오후 8시 이후
            logger.info("8PM이 되어 종료를 시작합니다.")
            stop_event.set()
            raise SystemExit("8PM 종료 완료")
        await asyncio.sleep(60)

@flow
def hun_fetch_and_send_stock_flow():
    stop_event = asyncio.Event()

    async def async_flow():
        logger = get_logger()
        
        if 'producer' not in globals():
            global producer
            producer = None
            
        try:
            connect_task = asyncio.create_task(run_connect.fn(stop_event))
            await check_time.fn(stop_event)  # 시간이 되면 SystemExit 발생
        except SystemExit as e:
            logger.info(f"종료 이벤트 발생: {e}")
        finally:
            stop_event.set()  # 모든 Task 종료 신호
            connect_task.cancel()
            try:
                await connect_task
            except asyncio.CancelledError:
                logger.info("Connect Task 강제 취소됨")

    asyncio.run(async_flow())

if __name__ == "__main__":
    hun_fetch_and_send_stock_flow()
