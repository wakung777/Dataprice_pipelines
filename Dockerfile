FROM apache/airflow:2.6.3-python3.10
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 libx11-xcb1 libxfixes3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
COPY requirement.txt .
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.3/constraints-3.10.txt" \
    apache-airflow-providers-postgres \
    -r requirement.txt
RUN playwright install chromium
