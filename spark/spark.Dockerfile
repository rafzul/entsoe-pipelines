#extending bitnami dockerfile
FROM bitnami/spark:3.3.1

ENV PYSPARK_PYTHON="python3.10"
ENV PYSPARK_DRIVER_PYTHON="python3.10"
ENV PYTHONPATH="${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-*-src.zip:${PYTHONPATH}"
ENV PATH="/.local/bin:$PATH"
RUN export PYSPARK_PYTHON
RUN export PYSPARK_DRIVER_PYTHON
RUN export PYTHONPATH
RUN export PATH

COPY ./spark/requirements_spark.txt /opt/spark/requirements_spark.txt
RUN pip3 install --no-cache-dir -r /opt/spark/requirements_spark.txt

COPY ./spark/spark-bigquery-with-dependencies_2.13-0.27.1.jar /opt/spark/jars/spark-bigquery-with-dependencies_2.13-0.27.1.jar
COPY ./spark/gcs-connector-hadoop3-latest.jar /opt/spark/jars/gcs-connector-hadoop3-latest.jar

