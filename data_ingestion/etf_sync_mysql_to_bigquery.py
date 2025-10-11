# -*- coding: utf-8 -*-
# data_ingestion/etf_sync_mysql_to_bigquery.py
import os
import pandas as pd
from sqlalchemy import create_engine

from data_ingestion.bigquery import get_bq_client, ensure_dataset, load_dataframe

# ---- MySQL 連線參數 ----
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB",   "ETF")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "app123")

def mysql_engine():
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    )
    # 注意：pandas 在某些版本組合下不吃 SA Connection，改用 raw_connection() 最穩
    return create_engine(url, pool_pre_ping=True, future=True)

# ---- BigQuery 參數 ----
PROJECT_ID   = os.getenv("GCP_PROJECT_ID")
DATASET_RAW  = os.getenv("BQ_DATASET_RAW", "etf_raw")
LOCATION     = os.getenv("BQ_LOCATION", "asia-east1")
WRITE_MODE   = os.getenv("BQ_LOAD_MODE", "replace")  # replace | append

def read_df_with_raw_conn(engine, sql: str, parse_dates=None) -> pd.DataFrame:
    """透過 DB-API raw connection 讓 pandas 讀取，避開 SA 版本相容性問題。"""
    raw = engine.raw_connection()
    try:
        return pd.read_sql_query(sql, raw, parse_dates=parse_dates)
    finally:
        raw.close()

def main():
    print(
        f"[ENV] MYSQL_HOST={MYSQL_HOST}, DB={MYSQL_DB}, "
        f"PROJECT={PROJECT_ID}, RAW_DATASET={DATASET_RAW}, "
        f"KEY={os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}"
    )

    eng = mysql_engine()
    bq  = get_bq_client(PROJECT_ID)
    ensure_dataset(bq, DATASET_RAW, location=LOCATION)

    # --- 讀 MySQL（三張表）— 用 raw_connection() + 純字串 SQL ---
    df_price = read_df_with_raw_conn(
        eng, "SELECT * FROM etf_day_price",
        parse_dates=["trade_date"]
    )
    df_dividend = read_df_with_raw_conn(
        eng, "SELECT * FROM etf_dividend",
        parse_dates=["ex_date", "record_date", "payable_date"]
    )
    df_metrics = read_df_with_raw_conn(
        eng, "SELECT * FROM etf_metrics_daily",
        parse_dates=["trade_date"]
    )

    # --- 上傳到 BigQuery（含分區/叢集設定） ---
    load_dataframe(
        bq, df_price, "etf_day_price", DATASET_RAW,
        write_mode=WRITE_MODE, partition_field="trade_date",
        clustering_fields=["ticker"]
    )
    load_dataframe(
        bq, df_dividend, "etf_dividend", DATASET_RAW,
        write_mode=WRITE_MODE, partition_field="ex_date",
        clustering_fields=["ticker"]
    )
    load_dataframe(
        bq, df_metrics, "etf_metrics_daily", DATASET_RAW,
        write_mode=WRITE_MODE, partition_field="trade_date",
        clustering_fields=["ticker"]
    )

    print("✅ MySQL → BigQuery 完成：etf_day_price / etf_dividend / etf_metrics_daily")

if __name__ == "__main__":
    main()
