import os
import logging
import traceback

from prefect import flow
from prefect.deployments import DeploymentImage
from prefect.client.schemas.schedules import CronSchedule

from flow import stock_data_pipeline


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
    stock_data_pipeline.deploy(
        name="hun_stock_deploy",
        work_pool_name="docker-agent-pool",
        work_queue_name="docker-agent",
        image=DeploymentImage(
            name="hun-stock",
            tag=os.getenv("VERSION"),
            dockerfile="Dockerfile",
            platform="linux/arm64",
            buildargs={
                        "LOGGING_LEVEL": os.getenv("LOGGING_LEVEL"),
                        "APP_KEY": os.getenv("APP_KEY"),
                        "APP_SECRET": os.getenv("APP_SECRET"),
                        "HTS_ID": os.getenv("HTS_ID"),
                        "KAFKA_URL": os.getenv("KAFKA_URL"),
                        },
        ),
        schedule=(CronSchedule(cron="0 8 * * 1-5", timezone="Asia/Seoul")),
        build=True,
    )