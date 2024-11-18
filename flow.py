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
        producer = KafkaProducer(acks=0,
                                 compression_type='gzip',
                                 bootstrap_servers=os.getenv('KAFKA_URL', 'default_url'),
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                 api_version=(2,)
                                 )
        logger.debug('Kafka Producer 설정: acks=0, compression type=gzip, bootstrap_servers')
    except Exception as e:
        logger.error(f"Kafka Producer 설정 중 오류 발생: {e}")
        raise

def get_config():
    logger = get_logger()
    logger.debug('get_config 호출됨')
    return {
        "appkey": os.getenv('APP_KEY', 'default_url'),
        "appsecret": os.getenv('APP_SECRET', 'default_url'),
        "htsid": os.getenv('HTS_ID', 'default_url'),
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
    logger.debug('kafka로 전송 시작')
    current_date = datetime.datetime.now().strftime("%Y%m%d")  # 현재 날짜를 YYYYMMDD 형식으로 가져옴
    data['날짜'] = current_date  # 데이터 사전에 날짜 키를 추가
    producer.send(get_config()["kafka_topic"], value=data)
    logger.debug('kafka로 데이터 전송 완료')

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

async def connect():
    logger = get_logger()
    try:
        logger.debug('websocket 연결 시작')
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

            while True:
                try:
                    data = await asyncio.wait_for(ws.recv(), timeout=10)
                    if data[0] in ['0', '1']:
                        recvstr = data.split('|')
                        trid0 = recvstr[1]
                        if trid0 == "H0STASP0":
                            await stockhoka(recvstr[3])
                    else:
                        jsonObject = json.loads(data)
                        trid = jsonObject["header"]["tr_id"]
                        if trid == "PINGPONG":
                            logger.debug(f"### RECV [PINGPONG] [{data}]")
                            logger.debug(f"### SEND [PINGPONG] [{data}]")
                            await ws.send(data)
                except asyncio.TimeoutError:
                    logger.debug("Timeout 발생: 재연결 시도 중")
                    continue
                except websockets.ConnectionClosed:
                    logger.warning("웹소켓 연결이 닫혔습니다. 재연결 시도 중...")
                    continue
    except Exception as e:
        logger.error(f"웹소켓 연결 오류: {e}. 재연결을 시도합니다.")
    finally:
        if producer:
            producer.close()
        logger.debug('websocket 연결 종료')

async def shutdown_at_8pm():
    logger = get_logger()
    try:
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.datetime.now(kst)
        target_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if now < target_time:
            wait_seconds = (target_time - now).total_seconds()
            logger.info(f"8PM KST까지 {wait_seconds}초 대기 중")
            await asyncio.sleep(wait_seconds)
        logger.info("한국 시간 오후 8시가 되어 프로그램을 종료합니다.")
        raise SystemExit("8PM 종료 완료")  # 시스템 종료 예외 발생
    except Exception as e:
        logger.error(f"shutdown_at_8pm에서 오류 발생: {e}")
        raise

async def run_connect():
    logger = get_logger()
    try:
        await connect()
    except asyncio.CancelledError:
        logger.info("Connect task가 취소되었습니다.")
        if producer:
            producer.close()
        raise
    except Exception as e:
        logger.error(f"Connect 실행 중 오류: {e}")
        raise

async def check_time():
    logger = get_logger()
    while True:
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.now(kst)
        if now.hour >= 20:  # 8PM 이후
            logger.info("8PM이 되어 종료를 시작합니다.")
            return True
        await asyncio.sleep(60) 

@flow
def hun_fetch_and_send_stock_flow():
    async def async_flow():
        logger = get_logger()

        if 'producer' not in globals():
            global producer
            producer = None

        try:
            connect_future = asyncio.create_task(run_connect.submit().result())
            check_future = asyncio.create_task(check_time.submit().result())

            # 시간 체크 태스크가 완료될 때까지 실행
            await check_future
            
            # connect 태스크 취소
            connect_future.cancel()
            try:
                await connect_future
            except asyncio.CancelledError:
                pass

            if producer:
                producer.close()
                logger.info("Producer가 정상적으로 종료되었습니다.")
            
            logger.info("모든 작업이 정상적으로 종료되었습니다.")
            
        except Exception as e:
            logger.error(f"Flow 실행 중 오류: {e}")
            if producer:
                producer.close()
            raise

    asyncio.run(async_flow())

if __name__ == "__main__":
   hun_fetch_and_send_stock_flow()