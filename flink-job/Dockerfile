FROM flink:1.17-java11

# Install Python + PyFlink
RUN apt-get update && apt-get install -y python3 python3-pip curl && \
    pip3 install apache-flink==1.17.2 && \
    rm -rf /var/lib/apt/lists/*

# Download Kafka connector JAR for Flink 1.17
RUN curl -L -o /opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar

WORKDIR /app
COPY job.py .
COPY submit.sh .
RUN chmod +x submit.sh

ENTRYPOINT ["/app/submit.sh"]
