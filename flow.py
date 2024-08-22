import websockets
import json
import requests
import os
import asyncio
from kafka import KafkaProducer
import logging
import datetime
import pytz
from prefect import task, flow


def setup_kafka_producer():
    global producer 

    try:
        producer = KafkaProducer(acks=0,
                                 compression_type='gzip',
                                 bootstrap_servers=os.getenv('KAFKA_URL', 'default_url'),
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                 api_version=(2,)
                                 )
        logging.info('Kafka Producer 설정: acks=0, compression type=gzip, bootstrap_servers')
    except Exception as e:
        logging.error(f"Kafka Producer 설정 중 오류 발생: {e}")
        raise

def get_config():
    logging.info('get_config 호출됨')
    return {
        "appkey": os.getenv('APP_KEY', 'default_url'),
        "appsecret": os.getenv('APP_SECRET', 'default_url'),
        "htsid": os.getenv('HTS_ID', 'default_url'),
        "kafka_topic": "tt_tick",
    }

def get_approval(key, secret):
    logging.info('get_approval 호출됨')
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

    if producer is None:
        setup_kafka_producer()
    logging.info('kafka로 전송 시작')
    current_date = datetime.datetime.now().strftime("%Y%m%d")  # 현재 날짜를 YYYYMMDD 형식으로 가져옴
    data['날짜'] = current_date  # 데이터 사전에 날짜 키를 추가
    producer.send(get_config()["kafka_topic"], value=data)
    logging.info('kafka로 데이터 전송 완료')

async def stockhoka(data):
    logging.info('stockhoka 처리 시작')
    recvvalue = data.split('^')
    await send_to_kafka({
        "종목코드": recvvalue[0],
        "현재가": recvvalue[3],
        "현재시간": recvvalue[1],
    })
    logging.info('stockhoka 처리 완료')

async def connect(shutdown_event):
    try:
        logging.info('websocket 연결 시작')
        config = get_config()
        g_approval_key = get_approval(config["appkey"], config["appsecret"])
        print(f"approval_key: {g_approval_key}")

        url = 'ws://ops.koreainvestment.com:31000'
        code_list = [
            ['1', 'H0STASP0', '005930'],
            ['1', 'H0STASP0', '051910'],
            ['1', 'H0STASP0', '000660'],
        ]
        senddata_list = [json.dumps({"header": {"approval_key": g_approval_key, "custtype": "P", "tr_type": i, "content-type": "utf-8"}, "body": {"input": {"tr_id": j, "tr_key": k}}}) for i, j, k in code_list]

        async with websockets.connect(url, ping_interval=None) as ws:
            for senddata in senddata_list:
                await ws.send(senddata)
                await asyncio.sleep(0.5)

            while not shutdown_event.is_set():
                try:
                    data = await ws.recv()
                    if data[0] in ['0', '1']:
                        recvstr = data.split('|')
                        trid0 = recvstr[1]
                        if trid0 == "H0STASP0":
                            await stockhoka(recvstr[3])
                    else:  # 웹소켓 세션 유지를 위한 PINGPONG 메시지 처리
                        jsonObject = json.loads(data)
                        trid = jsonObject["header"]["tr_id"]
                        if trid == "PINGPONG":
                            logging.debug(f"### RECV [PINGPONG] [{data}]")
                            logging.debug(f"### SEND [PINGPONG] [{data}]")
                            await ws.send(data)  # PINGPONG 메시지 응답
                except asyncio.TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    break
        logging.info('websocket 연결 종료')
    except Exception as e:
        logging.error(f"connect 중 오류 발생: {e}")
        raise
    finally:
        if producer:
            producer.close()
            
@task
async def run_connect(shutdown_event):
    await connect(shutdown_event)

@task
async def shutdown_at_8pm(shutdown_event):
    try:
        kst = pytz.timezone('Asia/Seoul')
        now = datetime.datetime.now(kst)
        logging.info(f"현재 시간: {now}")
        target_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if now < target_time:
            wait_seconds = (target_time - now).total_seconds()
            logging.info(f"8PM KST까지 {wait_seconds}초 대기 중")
            await asyncio.sleep(wait_seconds)
        logging.info("한국 시간 오후 8시가 되어 프로그램을 종료합니다.")
        shutdown_event.set()  # 종료 이벤트 설정

    except Exception as e:
        logging.error(f"shutdown_at_8pm에서 오류 발생: {e}")
        raise

@flow
def hun_fetch_and_send_stock_flow():
    async def async_flow():
        logging.basicConfig(level=logging.INFO, filename='app0416.log', filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        if 'producer' not in globals():
            global producer
            producer = None

        shutdown_event = asyncio.Event()
        connect_task = asyncio.create_task(run_connect(shutdown_event))
        shutdown_task = asyncio.create_task(shutdown_at_8pm(shutdown_event))
        
        await asyncio.gather(connect_task, shutdown_task)
        
        logging.info("모든 작업이 종료되었습니다.")

    asyncio.run(async_flow())


if __name__ == "__main__":
   hun_fetch_and_send_stock_flow() 

