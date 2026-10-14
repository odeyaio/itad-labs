from datetime import datetime
from urllib.parse import urlencode, urlsplit

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from ingestion.load_raw import load_to_raw

SOURCE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv?" + urlencode(
    {
        "$select": (
            "unique_key,created_date,closed_date,complaint_type,"
            "agency,borough,status,open_data_channel_type"
        ),
        "$where": (
            "borough = 'MANHATTAN' AND created_date >= '2025-01-01T00:00:00' "
            "AND created_date < '2025-02-01T00:00:00'"
        ),
        "$order": "created_date,unique_key",
        "$limit": "100000",
    }
)
SOURCE_FILENAME = "nyc311_manhattan_2025-01.csv"
DATASET_SLUG = "nyc311_manhattan"
source_parts = urlsplit(SOURCE_URL)

with DAG(
    dag_id="ingest_raw",
    description="Загрузка обращений NYC 311 в неизменяемый слой raw",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["lab-02", "raw", "nyc311"],
) as dag:
    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id="source_http_conn",
        endpoint=source_parts.path + ("?" + source_parts.query if source_parts.query else ""),
        method="HEAD",
        response_check=lambda response: response.status_code == 200,
        extra_options={"timeout": 30},
        poke_interval=60,
        timeout=600,
        mode="reschedule",
        deferrable=False,
    )

    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_url": SOURCE_URL,
            "source_filename": SOURCE_FILENAME,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": "s3_conn",
        },
    )

    wait_for_primary_source >> load_to_raw_task
