# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 你的爬蟲與指標計算模組
from data_ingestion.ETF_crawler_price import ETF_crawler_price
from data_ingestion.ETF_crawler_dividend import ETF_crawler_dividend
from data_ingestion import metrics_pipeline as mp  # 內含 init_db / get_engine / 三個步驟

# ---- 小封裝：跑 metrics_pipeline.py 的 main 流程 ----
def run_metrics():
    mp.init_db()
    eng = mp.get_engine()
    mp.build_adjusted_close(eng, threshold=mp.SPLIT_THRESHOLD)
    mp.create_metrics_view(eng)
    mp.upsert_materialized_table(eng)

default_args = {
    "owner": "data-team",
    "start_date": datetime(2025, 10, 8),       # 依需求調整；UTC 基準
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="ETF_crawler_etl_dag",
    description="TWSE ETF 價格/股利爬蟲 → 指標計算（metrics_pipeline）",
    default_args=default_args,
    schedule_interval="0 20 * * *",  
    catchup=False,
    max_active_runs=1,
    tags=["crawler", "etl", "metrics"],
) as dag:

    start = BashOperator(
        task_id="start_crawler",
        bash_command='echo "開始執行 ETF 爬蟲任務..."'
    )

    crawl_price = PythonOperator(
        task_id="crawl_price",
        python_callable=ETF_crawler_price,
        op_kwargs={
            "tickers": ["0050", "00881", "00922", "00692", "00850", "0056", "00878", "00919", "00713", "00929"],
            "start": "20251001",
            "end": "21001231",   # 寫長沒關係，你的函式會鎖到當月月初
            "sleep": 0.7,
        },
    )

    crawl_dividend = PythonOperator(
        task_id="crawl_dividend",
        python_callable=ETF_crawler_dividend,
        op_kwargs={
            "tickers": ["0050", "00881", "00922", "00692", "00850", "0056", "00878", "00919", "00713", "00929"],
            "start": "20200101",
            "end": "21001231",
            "sleep": 0.7,
        },
    )

    build_metrics = PythonOperator(
        task_id="build_metrics",
        python_callable=run_metrics,
        execution_timeout=timedelta(hours=2),   # 指標計算可能較久，給寬一點
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    end = BashOperator(
        task_id="end_crawler",
        bash_command='echo "爬蟲與指標任務完成！"',
        trigger_rule="all_success",
    )

    # 流程：start → [price, dividend] → metrics → end
    start >> [crawl_price, crawl_dividend] >> build_metrics >> end
