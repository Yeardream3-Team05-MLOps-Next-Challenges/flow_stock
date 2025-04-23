import os
import json
import requests
import websockets
import asyncio
import datetime
from typing import Optional, Dict, List
from kafka import KafkaProducer
import pytz

from src.logger import get_logger

async def process_market_data(data: str, producer: KafkaProducer, topic: str):
    """시장 데이터 처리 로직"""
    logger = get_logger()
    try:
        parsed_records = parse_data(data)
        if parsed_records:
            for record in parsed_records:
                try:
                    await send_to_kafka(record, producer, topic)
                except Exception as kafka_err:
                    logger.error(f"Kafka 전송 실패 (레코드: {record}): {kafka_err}")
    except Exception as e:
        logger.error(f"데이터 처리 실패: {e}")

def parse_data(raw_data: str) -> Optional[List[Dict]]: 
    """웹소켓 Raw 데이터를 파싱"""
    kst = pytz.timezone('Asia/Seoul')
    now_dt = datetime.datetime.now(kst)
    today_date_str = now_dt.strftime('%Y%m%d')

    parsed_records = [] 

    try:
        if raw_data.startswith('{'):
            try:
                json_data = json.loads(raw_data)
                tr_id = json_data.get('header', {}).get('tr_id')
                if tr_id == 'PINGPONG':
                    # print(f"[{now_dt.isoformat()}] DEBUG: PINGPONG 무시")
                    return None
                elif json_data.get('body', {}).get('msg_cd') == 'OPSP0000':
                    print(f"[{now_dt.isoformat()}] INFO: 구독 성공/실패 메시지 수신: {json_data.get('body', {}).get('msg1')}")
                    return None
                else:
                    print(f"[{now_dt.isoformat()}] WARNING: 알 수 없는 JSON 메시지 수신: {raw_data}")
                    return None
            except json.JSONDecodeError:
                print(f"[{now_dt.isoformat()}] ERROR: 잘못된 JSON 형식: {raw_data}")
                return None

        # 2. 파이프(|) 구분 메시지 확인
        elif raw_data.startswith('0|') or raw_data.startswith('1|'):
            parts = raw_data.split('|')
            if len(parts) >= 4:
                is_encrypted = parts[0] == '1'
                tr_id = parts[1]
                record_count_str = parts[2]
                body_data_raw = parts[3]

                # 3. TR_ID 확인 - 주식 체결(H0STCNT0)만 처리
                if tr_id == 'H0STCNT0':
                    if is_encrypted:
                        print(f"[{now_dt.isoformat()}] WARNING: 암호화된 H0STCNT0 데이터 수신 (처리 로직 없음)")
                        return None

                    # 데이터 건수 확인
                    try:
                        record_count = int(record_count_str)
                        if record_count <= 0:
                            return None 
                    except ValueError:
                        print(f"[{now_dt.isoformat()}] ERROR: 유효하지 않은 데이터 건수: {record_count_str}")
                        return None

                    all_fields = body_data_raw.split('^')
                    FIELDS_PER_RECORD = 46

                    # 필드 개수 검증
                    if len(all_fields) != record_count * FIELDS_PER_RECORD:
                        print(f"[{now_dt.isoformat()}] WARNING: 필드 개수 불일치. 예상: {record_count * FIELDS_PER_RECORD}, 실제: {len(all_fields)}. 데이터: {body_data_raw}")
                        return None 

                    # 여러 건의 데이터 처리
                    for i in range(record_count):
                        start_index = i * FIELDS_PER_RECORD
                        trade_data = all_fields[start_index : start_index + FIELDS_PER_RECORD]

                        stock_code = trade_data[0].strip()      
                        trade_time_str = trade_data[1].strip()  
                        current_price_str = trade_data[2].strip() 

                        # 간단한 유효성 검사
                        if not stock_code or not trade_time_str or not current_price_str:
                            print(f"[{now_dt.isoformat()}] WARNING: 필수 필드 누락 in record {i+1}. 데이터: {trade_data}")
                            continue 

                        # Kafka로 보낼 레코드 생성
                        record = {
                            "종목코드": stock_code,
                            "현재가": current_price_str,
                            "체결일시": f"{today_date_str}{trade_time_str}", # YYYYMMDDHHMMSS
                            "수신시간": now_dt.isoformat()
                        }
                        parsed_records.append(record)

                    # 처리된 레코드가 있으면 리스트 반환, 없으면 None 반환
                    return parsed_records if parsed_records else None

                else: 
                    return None
            else:
                print(f"[{now_dt.isoformat()}] WARNING: 잘못된 파이프(|) 구분 형식 (필드 부족): {raw_data}")
                return None
        else:
            print(f"[{now_dt.isoformat()}] WARNING: 알 수 없는 형식의 메시지 수신: {raw_data}")
            return None
    except Exception as e:
        print(f"[{now_dt.isoformat()}] ERROR: 데이터 파싱 중 예외 발생: {e} - Data: {raw_data}")
        return None

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
            ['1', 'H0STCNT0', '005930'],
            ['1', 'H0STCNT0', '051910'],
            ['1', 'H0STCNT0', '000660'],
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