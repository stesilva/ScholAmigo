FROM apache/airflow:2.10.5

ENV AIRFLOW_HOME=/opt/airflow

USER root
# Install Chrome & dependencies (required for Selenium)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        chromium:arm64 \
        chromium-driver:arm64 \
        libnss3 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libasound2 \
        libdrm2 \
        librdkafka-dev \
        wget \
        unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR $AIRFLOW_HOME

COPY scripts ./scripts/
RUN find scripts -type f -exec chmod +x {} \;
# RUN chmod +x /path/to/chromedriver

USER $AIRFLOW_UID
