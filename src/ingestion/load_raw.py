import hashlib
import logging
from typing import Any

import requests

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "raw"


def load_to_raw(
    *,
    source_url: str,
    source_filename: str,
    dataset_slug: str,
    s3_conn_id: str,
    **context: Any,
) -> str:
    key = f"{dataset_slug}/ingested_on={context['ds']}/{source_filename}"
    path = f"s3://{RAW_BUCKET}/{key}"
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    if not s3_hook.check_for_bucket(bucket_name=RAW_BUCKET):
        raise AirflowException(
            "Бакет raw недоступен. Выполните docker compose up -d "
            "и проверьте подключение s3_conn. DAG не создаёт бакеты."
        )

    if s3_hook.check_for_key(key=key, bucket_name=RAW_BUCKET):
        logging.info("Объект уже существует, загрузка пропущена: %s", path)
        return path

    with requests.get(
        source_url,
        headers={"Accept-Encoding": "identity"},
        stream=True,
        timeout=(15, 120),
    ) as response:
        response.raise_for_status()
        # raw.read сохраняет байты даже при Content-Encoding: gzip.
        payload = response.raw.read(decode_content=False)

    s3_hook.load_bytes(bytes_data=payload, key=key, bucket_name=RAW_BUCKET, replace=False)
    logging.info(
        "Сохранено %s, размер %d байт, SHA-256 %s",
        path,
        len(payload),
        hashlib.sha256(payload).hexdigest(),
    )
    return path
