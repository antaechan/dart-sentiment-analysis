FROM apache/airflow:3.0.2

WORKDIR /app

COPY requirements.txt .
COPY .env .
RUN pip install -r requirements.txt