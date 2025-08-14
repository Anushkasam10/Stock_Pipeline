Stock Market Data Pipeline with Airflow & Postgres

# Overview

This project is a data pipeline that fetches stock market data from the Alpha Vantage API, processes it, and stores it in a PostgreSQL database. The pipeline is orchestrated using Apache Airflow and containerized with Docker Compose.

# Features

- Automated Data Fetching : Retrieves daily adjusted stock data for multiple companies.
- ETL Pipeline : Extract → Transform → Load process using Python and Airflow.
- PostgreSQL Storage : Stores cleaned stock data for further analysis.
- Dockerized Deployment : Easy setup and execution with Docker Compose.

# Project Structure

stock-pipeline
│
├── docker-compose.yml # Docker setup file
├── dags
│ └── fetch_stocks_daily.py # Airflow DAG definition
├── scripts
│ └── fetch_data.py # Python script for data fetching
├── .env.example # Environment variable template
├── README.md # Project documentation

# Prerequisites

Before running the project, make sure you have:

- Docker and Docker Compose installed
- An Alpha Vantage API Key (Get one for free from: https://www.alphavantage.co/support/#api-key)

# Setup Instruction :

# Configure Environment Variables

Create a .env file in the root folder by copying .env.example :
bash
cp .env.example .env

Update the following values:
env
ALPHA_VANTAGE_API_KEY=your_api_key_here
SYMBOLS=TCS.NS,INFY.NS,RELIANCE.NS
POSTGRES_DB=stocks
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# Start the Services

bash
docker compose up -d

This will start:

- PostgreSQL on port `5432`
- Airflow Webserver on port `8080`
- Airflow Scheduler

# Access Airflow

Go to:

http://localhost:8080

Login with:

Username: admin
Password: admin

# Running the Pipeline

1. Turn on the fetch_stocks_daily DAG from the Airflow UI.
2. Trigger the DAG manually or wait for the schedule.
3. The pipeline will:
   - Fetch stock data from Alpha Vantage
   - Parse and transform it
   - Insert/Update records in PostgreSQL

# Verifying Data in PostgreSQL

Run:
bash
docker compose exec postgres psql -U airflow -d stocks -c "SELECT \* FROM candles ORDER BY ts DESC LIMIT 5;"

# Notes

- Ensure that your Alpha Vantage API key has not exceeded the daily limit.
- If the DAG task is skipped or fails, check logs in Airflow UI for error messages.
