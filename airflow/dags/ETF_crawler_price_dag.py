from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

# 導入爬蟲任務
from data_ingestion.ETF_crawler_price import ETF_crawler_price
from data_ingestion.ETF_crawler_dividend import ETF_crawler_dividend

# 預設參數
default_args = {
    'owner': 'data-team',
    'start_date': datetime(2025, 9, 28),
    'retries': 1,  # 失敗時重試 2 次
    'retry_delay': timedelta(minutes=1),  # 重試間隔 5 分鐘
    'execution_timeout': timedelta(hours=1),  # 執行超時時間 1 小時
}

# 建立 DAG
with DAG(
    dag_id='ETF_crawler_price_dag',
    default_args=default_args,
    description='證券交易所每日收盤資訊',
    schedule_interval='0 16 * * *',  # 每天 16 點執行
    catchup=False,  # 不執行歷史任務
    max_active_runs=1,  # 同時只允許一個 DAG 實例運行
    tags=['crawler', 'etl'],
) as dag:
    
        # 開始任務
    start_task = BashOperator(
        task_id='start_crawler',
        bash_command='echo "開始執行 ETF 爬蟲任務..."',
    )

    task_price = PythonOperator(
        task_id=f'crawl_price',
        python_callable=ETF_crawler_price,
        op_kwargs={
        "tickers": ["0050", "00881", "00922", "00692", "00850", "0056", "00878", "00919", "00713", "00929"],
        "start": "20200101",
        "end": "20250901",
        "sleep": 0.7,
    },
    )

    task_dividend = PythonOperator(
        task_id=f'crawl_dividend',
        python_callable=ETF_crawler_dividend,
        op_kwargs={
        "tickers": ["0050", "00881", "00922", "00692", "00850", "0056", "00878", "00919", "00713", "00929"],
        "start": "20200101",
        "end": "21001231",
        "sleep": 0.7,
    },
    )

        # 結束任務
    end_task = BashOperator(
        task_id='end_crawler',
        bash_command='echo "爬蟲任務執行完成！"',
        trigger_rule='all_success',
    )

    start_task >> [task_price, task_dividend] >> end_task