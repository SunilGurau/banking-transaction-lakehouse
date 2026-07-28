FROM apache/airflow:2.10.5-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Create an isolated Python environment for dbt
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/python -m pip install \
        --no-cache-dir \
        --upgrade pip setuptools wheel \
    && /home/airflow/dbt-venv/bin/python -m pip install \
        --no-cache-dir \
        dbt-core==1.9.8 \
        dbt-spark==1.9.0 \
        pyhive==0.7.0 \
        thrift==0.20.0

RUN python -m pip install --no-cache-dir \
        boto3 \
        apache-airflow-providers-amazon \
        deltalake \
        pandas \
        pyarrow

# Do not prepend the dbt venv to the global PATH.
# Validate each environment using its explicit executable.
RUN git --version \
    && /usr/local/bin/python -m pip check \
    && /home/airflow/dbt-venv/bin/python -m pip check \
    && /home/airflow/dbt-venv/bin/dbt --version

# initialize dbt project
# RUN /home/airflow/dbt-venv/bin/dbt init my_dbt_project

RUN pip install confluent-kafka