# Airflow image + Node.js + Chromium + gspread, so the workers can run the credit card scraper.
#
#   docker build -t airflow-credit-cards:3.4.0 test_projects/credit_cards
#   then set AIRFLOW_IMAGE_NAME=airflow-credit-cards:3.4.0 in ~/airflow/.env and `docker compose up -d`

# --- Stage 1: install the Node dependencies (Chromium comes from apt, so skip Puppeteer's download)
FROM node:24-trixie-slim AS scraper-deps
WORKDIR /opt/card-scraper
COPY scraper/package.json ./
ENV PUPPETEER_SKIP_DOWNLOAD=true
RUN npm install --omit=dev --no-audit --no-fund

# --- Stage 2: the Airflow image used by all Airflow services
FROM apache/airflow:3.4.0

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends chromium \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=scraper-deps /usr/local/bin/node /usr/local/bin/node
COPY --from=scraper-deps /opt/card-scraper/node_modules /opt/card-scraper/node_modules

# scrape.js lives in the mounted DAGs folder; its dependencies live here in the image.
ENV NODE_PATH=/opt/card-scraper/node_modules \
    CHROMIUM_PATH=/usr/bin/chromium

USER 50000
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "gspread>=6,<7"
