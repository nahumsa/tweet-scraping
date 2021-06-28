FROM apache/airflow:2.1.0
COPY req.txt .

USER root
# Install git
RUN sudo apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get purge -y --auto-remove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Install requirements
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --user -r req.txt