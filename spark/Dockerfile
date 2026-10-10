# Spark 3.5 + MongoDB Spark Connector 10.x
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Se resuelve el conector (y sus dependencias: driver de MongoDB y BSON) en tiempo
# de construcción y se copian a /opt/spark/jars. Así master, workers y driver
# tienen los mismos jars y no se descarga nada al ejecutar los trabajos.
ARG MONGO_SPARK_CONNECTOR=org.mongodb.spark:mongo-spark-connector_2.12:10.4.0
RUN echo "" > /tmp/noop.py \
 && /opt/spark/bin/spark-submit --master "local[1]" \
      --conf spark.jars.ivy=/tmp/ivy \
      --packages "${MONGO_SPARK_CONNECTOR}" /tmp/noop.py \
 && cp /tmp/ivy/jars/*.jar /opt/spark/jars/ \
 && ls /opt/spark/jars | grep -i -E "mongo|bson" \
 && rm -rf /tmp/ivy /tmp/noop.py

COPY jobs /opt/jobs
RUN chmod +x /opt/jobs/*.sh && mkdir -p /data && chown -R spark:spark /opt/jobs

USER spark
WORKDIR /opt/jobs
