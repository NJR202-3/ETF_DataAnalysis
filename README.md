ETF_DataAnalysis

以 台股 ETF 為核心的資料工程專案：從資料擷取、指標計算到 BigQuery / Metabase 視覺化，並以 Apache Airflow 編排整體流程。專案設計重視「本機先跑通、再上雲」的學習曲線。

🏗️ 架構總覽

資料來源：TWSE API（ETF 歷史日價、配息）

運算編排：Apache Airflow（每日排程）

資料庫：MySQL 8（搭配 phpMyAdmin）

資料倉儲：Google BigQuery（Raw + Analytics）

視覺化：Metabase（儀表板）

容器化：Docker & Docker Compose

套件管理：uv（更快的 Python 套件管理）

資料流程

TWSE → Python 爬蟲 → MySQL → metrics_pipeline → (BigQuery ELT) → Metabase
                     ↑                                   ↓
                  Airflow DAG                    Analytics views / tables

📁 專案結構
ETF_DataAnalysis/
├── data_ingestion/
│   ├── __init__.py
│   ├── mysql.py                            # MySQL 連線 & Schema & UPSERT
│   ├── ETF_crawler_dividend.py             # ETF 配息抓取（彙總同日多筆）
│   ├── ETF_crawler_price.py                # ETF 歷史日價（逐月抓取，連續空月停損）
│   ├── metrics_pipeline.py                 # 指標計算 → 物化到 etf_metrics_daily
│   ├── bigquery.py                         # BigQuery 共用工具
│   ├── etf_sync_mysql_to_bigquery.py       # MySQL → BigQuery Raw 同步
│   └── etf_bigquery_transform.py           # 在 BigQuery 建 View / 物化表
│
├── airflow/
│   ├── airflow.cfg
│   ├── Dockerfile
│   ├── docker-compose-airflow.yml
│   └── dags/
│       └── ETF_bigquery_etl_dag.py         # DAG: MySQL → BQ → 轉換
│
├── docker-compose-mysql.yml                # MySQL + phpMyAdmin
├── docker-compose-metabase.yml             # Metabase
├── .env                                    # 環境變數（不入版控）
├── pyproject.toml / uv.lock                # 依賴管理（uv）
└── README.md

🔧 先備環境

Python 3.10+、Docker Desktop、uv

建立 Docker 網路（供多服務互通）

docker network create my_network


安裝依賴

uv sync

🌍 環境變數（.env）

程式會讀取 .env，缺省時使用預設值。此檔 不要進版控。

# Docker Hub（Airflow 自行 build 的 image tag）
DOCKER_HUB_USER=chris

# MySQL（給你的程式/ETL 使用）
MYSQL_HOST=127.0.0.1        # 本機跑腳本用；Airflow 容器內會覆寫為 mysql
MYSQL_PORT=3306
MYSQL_DB=ETF
MYSQL_USER=app
MYSQL_PASSWORD=

# Metabase（應用設定 DB）
MB_DB_USER=metabase
MB_DB_PASS=

# Airflow
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASS=

# BigQuery / GCP
GCP_PROJECT_ID=etfproject20250923
BQ_DATASET_RAW=etf_raw
BQ_DATASET_ANALYTICS=etf_analytics
GOOGLE_APPLICATION_CREDENTIALS=/home/chris/ETF_DataAnalysis/key.json  # 本機路徑；Airflow 內會掛載到 /opt/airflow/key.json

# 指標與修正參數
SPLIT_THRESHOLD=0.20   # 拆/合股偵測（非除息日且跳動幅度）


注意

在 本機 執行 Python 腳本時，MYSQL_HOST=127.0.0.1。

在 Airflow 容器 執行時，DAG 會將 MYSQL_HOST 覆寫為 mysql（Compose 服務名）。

🗄️ 啟動服務
MySQL + phpMyAdmin
docker compose -f docker-compose-mysql.yml up -d
# phpMyAdmin: http://localhost:8000


Compose 內已設 --max_allowed_packet=128M，避免建立大型 VIEW 時斷線。

Metabase
docker compose -f docker-compose-metabase.yml up -d
# Web: http://localhost:3000
# 首次登入後新增 MySQL 連線（與 .env 相同）

Airflow（DAG 排程）
# build 專用 Airflow image（內含本專案依賴與 dags）
docker compose -f airflow/docker-compose-airflow.yml build --no-cache

# 啟動
docker compose -f airflow/docker-compose-airflow.yml up -d
# Web: http://localhost:8080  

# 首次初始化（若尚未 init）
docker compose -f airflow/docker-compose-airflow.yml up -d airflow-init

🚀 本機執行流程（不走 Airflow）

建議先載入 .env（或每條 uv run 後加 --env-file .env）

# 1) 載入環境變數
source .env

# 2) 抓 ETF 配息
uv run -m data_ingestion.ETF_crawler_dividend

# 3) 抓 ETF 歷史日價
uv run -m data_ingestion.ETF_crawler_price

# 4) 計算指標 → 物化 etf_metrics_daily
uv run -m data_ingestion.metrics_pipeline

☁️ BigQuery：輕量 ELT

目標只是「證明你會用 BigQuery」，因此採用 輕量 ELT：

同步：把 MySQL 三張表上傳至 BQ_DATASET_RAW

etf_day_price（分區：trade_date；叢集：ticker）

etf_dividend（分區：ex_date；叢集：ticker）

etf_metrics_daily（分區：trade_date；叢集：ticker）

轉換：在 BigQuery 建 視圖與物化表（供 Metabase / 查詢）

1) 本機直跑
# 安裝 BQ 相關套件（若未安裝）
uv add google-cloud-bigquery pandas pyarrow pandas-gbq pymysql SQLAlchemy

# 同步 MySQL → BigQuery Raw
uv run --env-file .env -m data_ingestion.etf_sync_mysql_to_bigquery

# 建 view / 物化表（Analytics）
uv run --env-file .env -m data_ingestion.etf_bigquery_transform

2) 用 Airflow 跑（推薦）

DAG：airflow/dags/ETF_bigquery_etl_dag.py

流程：

sync_mysql_to_bigquery  →  bigquery_transform


在容器內，DAG 會自動設定：

MYSQL_HOST=mysql（與 Compose 服務同名）

GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/key.json

🗄️ 資料表說明
etf_dividend（唯一鍵：ticker + ex_date）

ticker, short_name, ex_date, record_date, payable_date

cash_dividend（小數；同日多筆已彙總）

created_at, updated_at

etf_day_price（唯一鍵：ticker + trade_date）

volume, amount（整數）

open, high, low, close（小數）

adjusted_close（由 pipeline 計算）

trades, created_at, updated_at

etf_metrics_daily（唯一鍵：ticker + trade_date）

adjusted_close, daily_return

vol_252、sharpe_252d、tri_total_return

drawdown, mdd

dividend_12m, dividend_yield_12m

📐 指標說明（metrics_pipeline.py → etf_metrics_daily）

所有指標都以 還原價 adjusted_close 為基礎；每天一筆。

adjusted_close — 還原價
把拆/合股、配息等跳動調整掉，讓報酬率連續可比。

非除息日、且漲跌幅超過 SPLIT_THRESHOLD（預設 0.20）→ 視為拆/合股，向前回推比例調整。

除息日以現金股利做價格還原。

daily_return — 日報酬率
公式：(adj_t / adj_{t-1}) - 1；首日無前值為 NULL。

vol_252 — 年化波動度（252 交易日）
公式：std(daily_return_{252}) * sqrt(252)；視窗不足為 NULL。

sharpe_252d — 年化夏普值（252 交易日）
公式：(mean(daily_return_{252}) / std(daily_return_{252})) * sqrt(252)；RF 預設 0。

tri_total_return — 成立以來累積報酬（TRI-1）
公式：Π (1 + daily_return) - 1；自該檔 ETF 第一筆資料起累乘。

drawdown — 回撤
公式：adj_t / max(adj_{≤t}) - 1；新高為 0，低於新高為負值。

mdd — 最大回撤
公式：min(drawdown_{≤t})；例如 -0.45 代表歷史最深跌 45%。

dividend_12m — 近 12 個月現金股利總額
匯總 etf_dividend.cash_dividend 近 365 天（含當日）；無配息為 0。

dividend_yield_12m — 近 12 個月殖利率
公式：dividend_12m / adjusted_close；以當日價衡量 TTM 殖利率。

📊 Metabase 儀表板（建議）

啟動：docker compose -f docker-compose-metabase.yml up -d

新增 MySQL /（可選）BigQuery 連線

以 etf_metrics_daily 與 vw_metrics_daily 做探索圖表

快速面板：

單檔 ETF：近一年 adjusted_close、drawdown、dividend_yield_12m

橫向比較：多檔 ETF 的 vol_252、sharpe_252d、mdd

🛠️ 疑難排解

MySQL 斷線 / 大結果集
已在 Compose 設 --max_allowed_packet=128M；必要時重建：

docker compose -f docker-compose-mysql.yml up -d --force-recreate


Airflow DAG 一直失敗

檢查 Scheduler 容器內環境：

docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '
  grep -E "GCP_PROJECT_ID|GOOGLE_APPLICATION_CREDENTIALS|MYSQL_HOST" /opt/airflow/.env || true
  getent hosts mysql || true
  ls -l /opt/airflow/key.json || true
  python3 -c "import google.cloud.bigquery,pyarrow,pandas,pymysql,sqlalchemy;print(\"deps OK\")"
'


手動在容器內跑腳本看錯誤：

docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '
  set -a; source /opt/airflow/.env; set +a
  export PYTHONPATH=/opt/airflow
  python3 -m data_ingestion.etf_sync_mysql_to_bigquery
  python3 -m data_ingestion.etf_bigquery_transform
'


BigQuery 權限錯誤
確認 .env 的金鑰路徑與容器內 /opt/airflow/key.json 一致，並且 SA 具備 BigQuery Admin / Job User / Storage Viewer 等必要權限。

📦 版本控與提交

提醒：把 key.json、.env 加進 .gitignore，不要 push 憑證。

git add .
git commit -m "docs+feat: BigQuery 輕量 ELT、Airflow DAG、metrics 指標說明與啟動文件"
git push

📚 附錄：常用指令速查
# 套件
uv sync
uv add google-cloud-bigquery pandas pyarrow pandas-gbq pymysql SQLAlchemy

# 本機執行
source .env
uv run -m data_ingestion.ETF_crawler_dividend
uv run -m data_ingestion.ETF_crawler_price
uv run -m data_ingestion.metrics_pipeline
uv run -m data_ingestion.etf_sync_mysql_to_bigquery
uv run -m data_ingestion.etf_bigquery_transform

# Docker
docker network create my_network
docker compose -f docker-compose-mysql.yml up -d
docker compose -f docker-compose-metabase.yml up -d
docker compose -f airflow/docker-compose-airflow.yml build --no-cache
docker compose -f airflow/docker-compose-airflow.yml up -d