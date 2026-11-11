# Airflow 3.3.2 + an isolated virtualenv holding the pipeline and dbt, installed from uv.lock.
#
# Why a separate venv: Airflow pins its own dependency tree (httpx, jsonschema, ...). Installing
# our pins into Airflow's environment could break Airflow. Tasks call /opt/rift-venv/bin/python
# and /opt/rift-venv/bin/dbt instead, with exactly the versions tested locally (same lockfile).
FROM apache/airflow:3.3.2-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.10.11 /uv /usr/local/bin/uv

USER root
RUN mkdir -p /opt/rift-venv /opt/rift-build && chown -R airflow:root /opt/rift-venv /opt/rift-build
USER airflow

COPY --chown=airflow:root pyproject.toml uv.lock /opt/rift-build/
RUN cd /opt/rift-build \
    && UV_PROJECT_ENVIRONMENT=/opt/rift-venv UV_PYTHON_DOWNLOADS=never \
       uv sync --frozen --no-default-groups --group dbt --no-install-project --python python3.12 \
    && /opt/rift-venv/bin/python -c "import httpx, jsonschema, confluent_kafka, databricks.sdk" \
    && /opt/rift-venv/bin/dbt --version
