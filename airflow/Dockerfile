FROM apache/airflow:2.10.5-python3.12

USER airflow
WORKDIR /opt/airflow/project

COPY --chown=airflow:root pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/home/airflow/.local
RUN uv sync --locked --no-dev --no-install-project
