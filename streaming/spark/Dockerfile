FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

ARG SPARK_V=3.5.3
ARG KAFKA_V=3.4.1
ARG POOL_V=2.11.1
ARG HADOOP_V=3.3.4
ARG AWS_SDK_V=1.12.262

RUN pip install --no-cache-dir "pyspark==${SPARK_V}" "pymongo>=4.8,<5" "prometheus-client>=0.21,<1"

# Connecteur Kafka figé dans l'image : pas de résolution Maven au démarrage.
ENV SPARK_JARS=/usr/local/lib/python3.12/site-packages/pyspark/jars
RUN set -eux; base=https://repo1.maven.org/maven2; \
    curl -fsSL -o "$SPARK_JARS/spark-sql-kafka-0-10_2.12-${SPARK_V}.jar" \
        "$base/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_V}/spark-sql-kafka-0-10_2.12-${SPARK_V}.jar"; \
    curl -fsSL -o "$SPARK_JARS/spark-token-provider-kafka-0-10_2.12-${SPARK_V}.jar" \
        "$base/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_V}/spark-token-provider-kafka-0-10_2.12-${SPARK_V}.jar"; \
    curl -fsSL -o "$SPARK_JARS/kafka-clients-${KAFKA_V}.jar" \
        "$base/org/apache/kafka/kafka-clients/${KAFKA_V}/kafka-clients-${KAFKA_V}.jar"; \
    curl -fsSL -o "$SPARK_JARS/commons-pool2-${POOL_V}.jar" \
        "$base/org/apache/commons/commons-pool2/${POOL_V}/commons-pool2-${POOL_V}.jar"; \
    curl -fsSL -o "$SPARK_JARS/hadoop-aws-${HADOOP_V}.jar" \
        "$base/org/apache/hadoop/hadoop-aws/${HADOOP_V}/hadoop-aws-${HADOOP_V}.jar"; \
    curl -fsSL -o "$SPARK_JARS/aws-java-sdk-bundle-${AWS_SDK_V}.jar" \
        "$base/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V}/aws-java-sdk-bundle-${AWS_SDK_V}.jar"    

RUN useradd --create-home --uid 1000 spark \
    && mkdir -p /checkpoints \
    && chown -R spark:spark /checkpoints

USER spark

WORKDIR /app
COPY --chown=spark:spark telemetry_job.py .

CMD ["python", "telemetry_job.py"]