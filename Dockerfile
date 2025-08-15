FROM apache/airflow:3.0.2

WORKDIR /app

COPY requirements.txt .
COPY .env .

USER root


# 런타임 + ARM64용 Chromium/Driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget unzip ca-certificates gnupg \
    libnss3 libasound2 libx11-6 libx11-xcb1 libxcb1 \
    libxcomposite1 libxcursor1 libxdamage1 libxext6 libxi6 \
    libxrender1 libxrandr2 libxss1 libxtst6 libgbm1 libu2f-udev \
    fonts-liberation xdg-utils \
    chromium chromium-driver \
 && rm -rf /var/lib/apt/lists/*

# (선택) 경로 고정
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver


USER airflow

RUN pip install -r requirements.txt


