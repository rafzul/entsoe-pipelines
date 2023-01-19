# First-time build can take upto 10 mins.

FROM apache/airflow:2.5.0-python3.10
ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && \
    apt-get install vim -qqq \
    #install open jdk
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

#Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

#wget spark and extract
RUN wget https://www.apache.org/dyn/closer.lua/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3-scala2.13.tgz \
    && tar -xvzf spark-3.3.1-bin-hadoop3-scala2.13.tgz /opt/jdk

#set spark home
ENV SPARK_HOME /opt/jdk/spark-3.3.1-bin-hadoop3-scala2.13
RUN export SPARK_HOME


#install openjdk
# git gcc g++ -qqq\

COPY requirements.txt .
USER $AIRFLOW_UID
RUN pip install --no-cache-dir -r requirements.txt

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;


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

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID

