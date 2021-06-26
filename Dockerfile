FROM apache/airflow:2.1.0
COPY requirements.txt .

USER root
# Install git
RUN sudo apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get purge -y --auto-remove

# Install requirements
RUN pip install --upgrade pip
RUN pip install -r requirements.txt