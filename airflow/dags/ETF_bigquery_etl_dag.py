# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_DIR = "/opt/airflow"          # repo 掛載在容器內的位置
ENV_FILE = f"{REPO_DIR}/.env"      # 顯式載入 .env，避免環境不同步

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ETF_bigquery_etl_dag",
    description="Sync MySQL -> BigQuery then build analytics tables/views for ETF",
    start_date=datetime(2025, 1, 1),
    schedule="30 12 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["etl", "bigquery", "etf"],
) as dag:

    # 會把 .env 匯入，設定 PYTHONPATH，再用 python3 -m 執行
    sync_mysql_to_bigquery = BashOperator(
        task_id="sync_mysql_to_bigquery",
        bash_command=(
            "set -euo pipefail; set -x; "
            f"set -a; source {ENV_FILE}; set +a; "
            f"export PYTHONPATH={REPO_DIR}; "
            "echo 'PYTHONPATH='$PYTHONPATH; "
            "echo 'GOOGLE_APPLICATION_CREDENTIALS='$GOOGLE_APPLICATION_CREDENTIALS; "
            "ls -l \"$GOOGLE_APPLICATION_CREDENTIALS\" || true; "
            "python3 -m data_ingestion.etf_sync_mysql_to_bigquery"
        ),
    )

    bigquery_transform = BashOperator(
        task_id="bigquery_transform",
        bash_command=(
            "set -euo pipefail; set -x; "
            f"set -a; source {ENV_FILE}; set +a; "
            f"export PYTHONPATH={REPO_DIR}; "
            "python3 -m data_ingestion.etf_bigquery_transform"
        ),
    )

    sync_mysql_to_bigquery >> bigquery_transform
