FROM apache/airflow:3.0.2

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt