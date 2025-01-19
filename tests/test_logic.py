import pytest
from unittest.mock import patch, MagicMock
from src.logic import (
    get_config_logic,
    setup_kafka_producer_logic,
    get_approval_logic,
    send_to_kafka_logic,
    stockhoka_logic
)
from kafka import KafkaProducer

# get_config_logic 테스트
def test_get_config_logic():
    config = get_config_logic()
    assert isinstance(config, dict)
    assert "appkey" in config
    assert "appsecret" in config
    assert "htsid" in config
    assert "KAFKA_TOPIC" in config
    assert "CODE_LIST" in config
    assert "WEBSOCKET_URL" in config
    assert "API_URL" in config
    

# setup_kafka_producer_logic 테스트
@patch('src.logic.KafkaProducer')
def test_setup_kafka_producer_logic(MockKafkaProducer):
    producer = setup_kafka_producer_logic()
    assert isinstance(producer, MagicMock)
    MockKafkaProducer.assert_called_once()

# get_approval_logic 테스트
@patch('src.logic.requests.post')
def test_get_approval_logic(mock_post):
    mock_response = MagicMock()
    mock_response.json.return_value = {"approval_key": "test_approval_key"}
    mock_post.return_value = mock_response

    approval_key = get_approval_logic("test_key", "test_secret")
    assert approval_key == "test_approval_key"

# send_to_kafka_logic 테스트
@pytest.mark.asyncio
@patch('src.logic.KafkaProducer')
async def test_send_to_kafka_logic(MockKafkaProducer):
     mock_producer = MockKafkaProducer.return_value
     data = {"test": "data"}
     producer = setup_kafka_producer_logic()
     await send_to_kafka_logic(data, producer)
     mock_producer.send.assert_called_once()

# stockhoka_logic 테스트
@pytest.mark.asyncio
@patch('src.logic.send_to_kafka_logic')
async def test_stockhoka_logic(mock_send_to_kafka_logic):
    test_data = "005930^165000^1000^70000"
    producer = setup_kafka_producer_logic()
    await stockhoka_logic(test_data, producer)
    mock_send_to_kafka_logic.assert_called_once()