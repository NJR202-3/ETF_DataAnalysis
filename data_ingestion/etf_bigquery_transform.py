# -*- coding: utf-8 -*-
# data_ingestion/etf_bigquery_transform.py
import os
from data_ingestion.bigquery import (
    get_bq_client, ensure_dataset,
    create_or_replace_view, create_or_replace_table_as_select
)

PROJECT_ID   = os.getenv("GCP_PROJECT_ID")
DATASET_RAW  = os.getenv("BQ_DATASET_RAW", "etf_raw")
DATASET_AN   = os.getenv("BQ_DATASET_ANALYTICS", "etf_analytics")
LOCATION     = os.getenv("BQ_LOCATION", "asia-east1")

def main():
    print(f"[ENV] PROJECT={PROJECT_ID}, RAW={DATASET_RAW}, AN={DATASET_AN}")
    client = get_bq_client(PROJECT_ID)
    ensure_dataset(client, DATASET_AN, location=LOCATION)

    # 1) 直接把 MySQL 預算好的 metrics 投影成 View
    create_or_replace_view(
        client, DATASET_AN, "vw_metrics_daily",
        f"""
        SELECT
          ticker, trade_date, adjusted_close, daily_return,
          tri_total_return, vol_252, sharpe_252d,
          drawdown, mdd, dividend_12m, dividend_yield_12m
        FROM `{client.project}.{DATASET_RAW}.etf_metrics_daily`
        """
    )

    # 2) 各 ETF 最新一日快照（物化表）
    create_or_replace_table_as_select(
        client, DATASET_AN, "metrics_latest",
        f"""
        WITH lastday AS (
          SELECT ticker, MAX(trade_date) AS last_dt
          FROM `{client.project}.{DATASET_RAW}.etf_metrics_daily`
          GROUP BY ticker
        )
        SELECT m.*
        FROM `{client.project}.{DATASET_RAW}.etf_metrics_daily` m
        JOIN lastday d
          ON m.ticker = d.ticker AND m.trade_date = d.last_dt
        """
    )

    # 3) 可選：把原始表映成 View
    for name in ("etf_day_price", "etf_dividend"):
        create_or_replace_view(
            client, DATASET_AN, f"vw_{name}",
            f"SELECT * FROM `{client.project}.{DATASET_RAW}.{name}`"
        )

    print("✅ BigQuery 轉換完成：vw_metrics_daily / metrics_latest / vw_*")

if __name__ == "__main__":
    main()
