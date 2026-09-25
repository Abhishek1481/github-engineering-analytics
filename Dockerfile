FROM apache/airflow:3.3.2-python3.11

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.11

# Install the pipeline package *within Airflow's constraint set* so it can
# never up/downgrade Airflow's own pinned dependencies. Editable install keeps
# /opt/project/sql and /opt/project/config next to the code.
COPY --chown=airflow:root . /opt/project
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -e /opt/project \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ENV PIPELINE_CONFIG=/opt/project/config/pipeline.yml
