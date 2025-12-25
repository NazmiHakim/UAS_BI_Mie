FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    libpq-dev \
    build-essential \
    cron \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY docker/crontab /etc/cron.d/simple-cron

RUN dos2unix /etc/cron.d/simple-cron

RUN chmod 0644 /etc/cron.d/simple-cron

RUN crontab /etc/cron.d/simple-cron

RUN touch /var/log/cron.log

COPY docker/entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]