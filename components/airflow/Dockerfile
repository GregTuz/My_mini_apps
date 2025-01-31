FROM apache/airflow:2.7.3
USER root

RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/

RUN mkdir -p /var/lib/apt/lists/partial && \
    chmod 755 /var/lib/apt/lists && \
    chmod 755 /var/lib/apt/lists/partial

RUN apt-get update && \
    apt-get install -y libpq-dev wget && \
    apt-get install -y build-essential gcc python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get update

USER airflow
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip install -r requirements.txt