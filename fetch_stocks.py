import os, time, logging
from datetime import datetime, timedelta
from typing import Dict, List

import requests
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def ensure_table(cur):
    cur.execute("""            CREATE TABLE IF NOT EXISTS candles (
          symbol TEXT NOT NULL,
          ts DATE NOT NULL,
          open NUMERIC,
          high NUMERIC,
          low NUMERIC,
          close NUMERIC,
          volume BIGINT,
          PRIMARY KEY (symbol, ts)
        );
    """)

def fetch_from_alpha_vantage(symbol: str, api_key: str, function: str, outputsize: str) -> Dict:
    base = "https://www.alphavantage.co/query"
    params = {
        "function": function,
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": outputsize
    }
    r = requests.get(base, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "Error Message" in data:
        raise RuntimeError(f"API error for {symbol}: {data['Error Message']}")
    if "Note" in data:
        logging.warning(f"Rate limited for {symbol}: {data['Note']}")
        raise RuntimeError("Rate limit; retry")
    return data

def parse_daily_adjusted_json(symbol: str, payload: Dict) -> List[tuple]:
    key = "Time Series (Daily)"
    if key not in payload:
        raise AirflowSkipException(f"Missing '{key}' in API response for {symbol}")
    rows = []
    for dt_str, ohlc in payload[key].items():
        # guard against missing fields
        try:
            open_v = float(ohlc.get("1. open", "nan"))
            high_v = float(ohlc.get("2. high", "nan"))
            low_v = float(ohlc.get("3. low", "nan"))
            close_v = float(ohlc.get("4. close", ohlc.get("5. adjusted close", "nan")))
            vol_v = int(float(ohlc.get("6. volume", "0")))
        except Exception as e:
            logging.warning(f"Bad row for {symbol} {dt_str}: {e}")
            continue

        rows.append((
            symbol,
            datetime.strptime(dt_str, "%Y-%m-%d").date(),
            open_v, high_v, low_v, close_v, vol_v
        ))
    return rows

def upsert_rows(conn, rows: List[tuple]):
    with conn.cursor() as cur:
        execute_values(cur, """                INSERT INTO candles (symbol, ts, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (symbol, ts) DO UPDATE
              SET open=EXCLUDED.open,
                  high=EXCLUDED.high,
                  low=EXCLUDED.low,
                  close=EXCLUDED.close,
                  volume=EXCLUDED.volume;
        """, rows)

def task_fetch_parse_upsert(**context):
    api_key = os.environ["ALPHA_VANTAGE_API_KEY"]
    function = os.getenv("AV_FUNCTION", "TIME_SERIES_DAILY_ADJUSTED")
    outputsize = os.getenv("AV_OUTPUTSIZE", "compact")
    symbols = [s.strip() for s in os.getenv("SYMBOLS", "IBM").split(",") if s.strip()]

    pg_conn = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
    )
    pg_conn.autocommit = False

    try:
        with pg_conn.cursor() as cur:
            ensure_table(cur)
        for i, sym in enumerate(symbols):
            if i > 0:
                time.sleep(12)
            data = fetch_from_alpha_vantage(sym, api_key, function, outputsize)
            rows = parse_daily_adjusted_json(sym, data)
            if not rows:
                logging.warning(f"No rows parsed for {sym}, skipping.")
                continue
            upsert_rows(pg_conn, rows)
        pg_conn.commit()
    except Exception as e:
        pg_conn.rollback()
        logging.exception("Pipeline failed, rolled back.")
        raise
    finally:
        pg_conn.close()

with DAG(
    dag_id="fetch_stocks_daily",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "ingestion"],
) as dag:

    fetch_and_upsert = PythonOperator(
        task_id="fetch_parse_upsert",
        python_callable=task_fetch_parse_upsert,
        provide_context=True,
    )
