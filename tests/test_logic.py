import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from src.logic import (
    handle_messages,
    load_config,
    get_api_approval,
    connect_websocket,
    process_market_data,
    parse_data,
    add_timestamp,
    send_to_kafka,
    monitor_shutdown
)
from kafka import KafkaProducer
import asyncio

# Config 테스트
def test_load_config():
    config = load_config()
    assert isinstance(config, dict)
    assert "APP_KEY" in config
    assert "APP_SECRET" in config
    assert "WEBSOCKET_URL" in config
    assert "KAFKA_TOPIC" in config

# API 인증 테스트
@patch('src.logic.requests.post')
def test_get_api_approval(mock_post):
    mock_response = MagicMock()
    mock_response.json.return_value = {"approval_key": "test_key"}
    mock_post.return_value = mock_response

    result = get_api_approval(
        "https://api.test.com",
        "test_key",
        "test_secret"
    )
    assert result == "test_key"
    mock_post.assert_called_once()

# 데이터 파싱 테스트
def test_parse_data():
    test_data = "005930|20230815123000|1000|70000"
    parsed = parse_data(test_data)
    assert parsed["종목코드"] == "005930"
    assert parsed["현재가"] == "70000"

# 타임스탬프 추가 테스트
def test_add_timestamp():
    data = {"종목코드": "005930"}
    result = add_timestamp(data)
    assert "수신시간" in result
    assert "종목코드" in result

# Kafka 전송 테스트
@pytest.mark.asyncio
async def test_send_to_kafka():
    mock_producer = MagicMock()
    test_data = {"종목코드": "005930"}
    
    await send_to_kafka(test_data, mock_producer, "test_topic")
    mock_producer.send.assert_called_once_with("test_topic", value=test_data)

# 웹소켓 핸들러 테스트
@pytest.mark.asyncio
@patch('src.logic.process_market_data')
async def test_handle_messages(mock_process):
    mock_ws = AsyncMock()
    mock_ws.recv.side_effect = [
        "005930|20230815123000|1000|70000",
        Exception("break")
    ]
    
    stop_event = asyncio.Event()
    await handle_messages(mock_ws, MagicMock(), "topic", stop_event)
    mock_process.assert_called_once()

# 종료 모니터링 테스트 
@pytest.mark.asyncio
async def test_monitor_shutdown():
    stop_event = asyncio.Event()
    task = asyncio.create_task(monitor_shutdown(stop_event))
    await asyncio.sleep(0.1)
    stop_event.set()
    assert stop_event.is_set()