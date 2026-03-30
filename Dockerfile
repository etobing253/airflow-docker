FROM apache/airflow:2.11.1

USER root

# 1. Install library dasar & dependencies Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 ca-certificates build-essential libpq-dev \
    # Dependencies untuk Playwright
    libnss3 libnspr4 libasound2 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
    libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
    # Tambahan untuk Oracle & MySQL
    libaio1 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 2. Install GCloud CLI dengan cara yang paling simpel
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

USER airflow

# 3. Install Python Packages
# Gunakan pymssql karena tidak butuh driver ODBC msodbcsql18 yang bikin error tadi
RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    apache-airflow-providers-oracle \
    apache-airflow-providers-microsoft-mssql \
    apache-airflow-providers-mysql \
    apache-airflow-providers-mongo \
    oracledb \
    pymssql \
    mysql-connector-python \
    pymongo \
    playwright \
    pandas \
    yfinance \
    jinja2 

# 4. Install Playwright Chromium
RUN playwright install chromium