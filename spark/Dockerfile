FROM apache/spark:3.5.1

USER root

ENV SPARK_HOME=/opt/spark
ENV DELTA_VERSION=3.2.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# Ship our Spark config (Delta + MinIO settings)
COPY conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

# Pre-fetch Delta Lake and S3A (MinIO) JARs so the Thrift Server works offline
RUN cd $SPARK_HOME/jars && \
    curl -fLO https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar && \
    curl -fLO https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar && \
    curl -fLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Thrift JDBC/ODBC port + Spark UI
EXPOSE 10000 4040

# Use spark-submit directly (foreground process) instead of start-thriftserver.sh
# which is a daemon launcher that forks and exits — causing Docker to kill the container.
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2", \
  "--name", "Thrift JDBC/ODBC Server", \
  "--conf", "spark.driver.host=spark-thrift", \
  "--hiveconf", "hive.server2.thrift.port=10000", \
  "--hiveconf", "hive.server2.thrift.bind.host=0.0.0.0", \
  "--hiveconf", "hive.server2.authentication=NOSASL"]