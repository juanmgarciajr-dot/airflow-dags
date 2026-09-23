from datetime import datetime
import json
import logging

import boto3
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)

api_base_url = "https://jsonplaceholder.typicode.com"
kinesis_client = boto3.client("kinesis")


def _set_api_user_id():
    try:
        current_user_id = int(
            Variable.get("api_user_id", default_var="-1")
        )

        if current_user_id in (-1, 10):
            new_user_id = 1
        else:
            new_user_id = current_user_id + 1

        # Airflow Variable values must be strings.
        Variable.set(
            key="api_user_id",
            value=str(new_user_id),
        )

        logger.info(
            "API user ID changed from %s to %s",
            current_user_id,
            new_user_id,
        )

        # PythonOperator automatically stores this return value in XCom.
        return new_user_id

    except Exception as error:
        logger.exception("Error while setting API user ID")
        raise RuntimeError(
            f"Error while setting API user ID: {error}"
        ) from error


def _extract_userposts(new_api_user_id, **context):
    try:
        user_id = int(new_api_user_id)

        logger.info(
            "Fetching posts for user ID %s",
            user_id,
        )

        response = requests.get(
            f"{api_base_url}/posts",
            params={"userId": user_id},
            timeout=30,
        )
        response.raise_for_status()

        user_posts = response.json()

        logger.info(
            "Retrieved %s posts for user ID %s",
            len(user_posts),
            user_id,
        )

        # The return value is automatically saved to XCom.
        return user_posts

    except Exception as error:
        logger.exception("Error while fetching user posts")
        raise RuntimeError(
            f"Error while fetching user posts: {error}"
        ) from error


def _process_user_posts(new_api_user_id, **context):
    try:
        stream_name = "user-posts-data-stream"

        # Pull the automatic return_value from _extract_userposts.
        user_posts = context["ti"].xcom_pull(
            task_ids="extract_userposts"
        )

        if not user_posts:
            logger.warning(
                "No posts were returned for user ID %s",
                new_api_user_id,
            )
            return "No posts were written to Kinesis"

        for user_post in user_posts:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=(
                    json.dumps(user_post) + "\n"
                ).encode("utf-8"),
                PartitionKey=str(user_post["userId"]),
            )

            logger.info(
                "Produced Kinesis record %s to shard %s",
                response["SequenceNumber"],
                response["ShardId"],
            )

        return (
            f"{len(user_posts)} posts for user ID "
            f"{new_api_user_id} were written to "
            f"Kinesis stream {stream_name}"
        )

    except Exception as error:
        logger.exception(
            "Error while writing user posts to Kinesis"
        )
        raise RuntimeError(
            f"Error while writing user posts to Kinesis: {error}"
        ) from error


with DAG(
    dag_id="load_api_aws_kinesis",
    default_args={"owner": "Sovan"},
    tags=["API data load to Kinesis"],
    start_date=datetime(2023, 9, 24),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    get_api_userId_params = PythonOperator(
        task_id="get_api_userId_params",
        python_callable=_set_api_user_id,
    )

    extract_userposts = PythonOperator(
        task_id="extract_userposts",
        python_callable=_extract_userposts,
        op_kwargs={
            "new_api_user_id": (
                "{{ ti.xcom_pull("
                "task_ids='get_api_userId_params') }}"
            )
        },
    )

    write_userposts_to_stream = PythonOperator(
        task_id="write_userposts_to_stream",
        python_callable=_process_user_posts,
        op_kwargs={
            "new_api_user_id": (
                "{{ ti.xcom_pull("
                "task_ids='get_api_userId_params') }}"
            )
        },
    )

    (
        get_api_userId_params
        >> extract_userposts
        >> write_userposts_to_stream
    )
