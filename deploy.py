import os
import logging
import traceback

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect.client.schemas.schedules import CronSchedule
from prefect.exceptions import PrefectException

from flow import hun_fetch_and_send_stock_data

logging.getLogger("prefect").setLevel(logging.DEBUG)

# @flow(name="fetch_and_send_stock_data_flow", log_prints=True)
# async def hun_stock_flow():
#     await fetch_and_send_stock_data()
    
#     # 슬랙 웹훅 URL
#     webhook_url = os.getenv("SLACK_WEBHOOK")

#     # 전송할 메시지
#     message = {
#         'text': 'stock이 실행되었습니다'
#     }

#     # HTTP POST 요청을 통해 메시지 전송
#     response = requests.post(
#         webhook_url,
#         data=json.dumps(message),
#         headers={'Content-Type': 'application/json'}
#     )

#     # 응답 상태 코드 출력
#     print('응답 상태 코드:', response.status_code)
#     print('응답 내용:', response.text)

if __name__ == "__main__":
    try:
        hun_fetch_and_send_stock_data.deploy(
            name="hun_tick2min_deploy",
            work_pool_name="docker-agent-pool",
            work_queue_name="docker-agent",
            image=DeploymentImage(
                name="hun-stock",
                tag="0.1.0",
                dockerfile="Dockerfile",
                platform="linux/arm64",
                buildargs={
                            "APP_KEY": os.getenv("APP_KEY"),
                            "APP_SECRET": os.getenv("APP_SECRET"),
                            "HTS_ID": os.getenv("HTS_ID"),
                            "KAFKA_URL": os.getenv("KAFKA_URL"),
                            },
            ),
            schedule=(CronSchedule(cron="0 8 * * *", timezone="Asia/Seoul")),
            build=True,
        )
    except PrefectException as e:
        print(f"Prefect error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(traceback.format_exc())