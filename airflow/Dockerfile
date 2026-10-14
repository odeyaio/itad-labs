FROM apache/airflow:2.10.5-python3.12

USER airflow
WORKDIR /opt/airflow/project

COPY --chown=airflow:root pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/home/airflow/.local
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN uv export --locked --no-dev --no-emit-project \
    --prune dbt-duckdb --prune duckdb --output-file .airflow-requirements.txt \
    && UV_HTTP_TIMEOUT=600 UV_CONCURRENT_DOWNLOADS=4 \
    uv pip sync --require-hashes --index-url ${PIP_INDEX_URL} \
    .airflow-requirements.txt \
    && rm .airflow-requirements.txt

COPY --from=apache/airflow:2.10.5-python3.12 /home/airflow/.local/bin/uv /usr/local/bin/uv
