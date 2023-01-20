# First-time build can take upto 10 mins.

FROM apache/airflow:2.5.0-python3.10
ENV AIRFLOW_HOME=/opt/airflow

#define spark version
ARG SPARK_VERSION="3.3.1"
ARG HADOOP_VERSION="3"
ARG SCALA_VERSION="2.13"

USER root
RUN apt-get update -qq && \
    apt-get install vim -qqq && \
    #install open jdk
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get install -y wget && \
    apt-get clean;


COPY requirements.txt .
USER $AIRFLOW_UID
RUN pip install --no-cache-dir -r requirements.txt

USER root
#Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME


###############################
## SPARK files and variables
###############################

#set spark files and variables
RUN cd 
ENV SPARK_HOME "/usr/local/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}"

#wget spark and extract
RUN cd "/tmp" && \
    mkdir -p "${SPARK_HOME}" && \
    wget --no-verbose "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz" && \
    tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz" -C "${SPARK_HOME}" --strip-components=1

#create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH "${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin:${PATH}"

#setup python path for spark
ENV PYSPARK_PYTHON "/usr/bin/python3.10"
ENV PYSPARK_DRIVER_PYTHON "/usr/bin/python3.10"
ENV PYTHONPATH "${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-*-src.zip:${PYTHONPATH}"
RUN export PYSPARK_PYTHON
RUN export PYSPARK_DRIVER_PYTHON
RUN export PYTHONPATH

#install openjdk
# git gcc g++ -qqq\



# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html
USER root

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=413.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"



RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
    --bash-completion=false \
    --path-update=false \
    --usage-reporting=false \
    --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

RUN chown -R airflow: ${AIRFLOW_HOME}
EXPOSE 8181

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID

