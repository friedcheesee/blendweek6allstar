FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

USER root

#curl
RUN apt-get update && apt-get install -y curl

#jdbc driver
RUN curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app

COPY spark_etl.py /app/spark_etl.py

CMD ["/opt/spark/bin/spark-submit", "/app/spark_etl.py"]
