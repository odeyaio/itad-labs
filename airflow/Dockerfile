FROM apache/airflow:2.10.5-python3.12

USER airflow
WORKDIR /opt/airflow/project

COPY --chown=airflow:root pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/home/airflow/.local
RUN uv export --locked --no-dev --no-emit-project \
    --prune dbt-duckdb --prune duckdb --output-file .airflow-requirements.txt \
    && uv pip sync --require-hashes .airflow-requirements.txt \
    && rm .airflow-requirements.txt

COPY --from=apache/airflow:2.10.5-python3.12 /home/airflow/.local/bin/uv /usr/local/bin/uv
