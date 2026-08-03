FROM apache/airflow:2.10.5-python3.12

USER root

# Install system dependencies (git and default Java) in a single clean layer
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME globally for the default Java installation
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# default-jdk on Debian bookworm installs OpenJDK 17. Spark 3.5 + Hadoop's
# reflection-based internals (S3A, UGI, etc.) throw
# java.lang.reflect.InaccessibleObjectException on JDK 17 without these
# module-opens flags. JAVA_TOOL_OPTIONS is picked up automatically by every
# JVM this container starts (spark-submit driver, executors, thriftserver),
# so this fixes it once instead of per-script. (The JVM will print
# "Picked up JAVA_TOOL_OPTIONS" to stderr on every launch -- harmless noise.)
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

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

# Install core python packages required for your lakehouse pipeline
# pyspark is version-pinned deliberately: the Spark jobs hardcode Maven
# coordinates (spark-sql-kafka-0-10_2.12:3.5.1) that must match the pyspark
# runtime version exactly, or you get binary-incompatible JAR errors.
#
# kafka-python-ng (not kafka-python) is deliberate too: kafka-python's last
# PyPI release is 2.0.2 from 2020, which vendors an old copy of `six` that
# throws ModuleNotFoundError: No module named 'kafka.vendor.six.moves' on
# Python 3.12 (this image's Python version) -- the upstream maintainers
# haven't been able to publish a fix. kafka-python-ng is the actively
# maintained fork the community uses in the meantime; it's a drop-in
# replacement (same `from kafka import KafkaProducer` import), so no
# application code changes are needed.
RUN python -m pip install --no-cache-dir \
        boto3 \
        apache-airflow-providers-amazon \
        deltalake \
        pandas \
        pyarrow \
        kafka-python-ng==2.2.3 \
        delta-spark==3.1.0 \
        pyspark==3.5.1

# Validate environments
RUN git --version \
    && /usr/local/bin/python -m pip check \
    && /home/airflow/dbt-venv/bin/python -m pip check \
    && /home/airflow/dbt-venv/bin/dbt --version