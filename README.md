# ETF_DataAnalysis

以 **台股 ETF** 為核心的資料工程專案：從資料擷取、指標計算到 **BigQuery / Metabase** 視覺化，並以 **Apache Airflow** 編排整體流程。專案設計重視「**本機先跑通、再上雲**」的學習曲線。

---

## 🏗️ 架構總覽

* **資料來源**：TWSE API（ETF 歷史日價、配息）
* **運算編排**：Apache Airflow（每日排程）
* **資料庫**：MySQL 8（搭配 phpMyAdmin）
* **資料倉儲**：Google BigQuery（Raw + Analytics）
* **視覺化**：Metabase（儀表板）
* **容器化**：Docker & Docker Compose
* **套件管理**：uv（更快的 Python 套件管理）

### 資料流程

```
TWSE → Python 爬蟲 → MySQL → metrics_pipeline
                ↓
        (BigQuery ELT 同步與轉換)
                ↓
             Metabase
        ↑                 ↓
      Airflow DAG   Analytics views / tables
```

---

## 📁 專案結構

```
ETF_DataAnalysis/
├── data_ingestion/
│   ├── __init__.py
│   ├── mysql.py                       # MySQL 連線 & Schema & UPSERT
│   ├── ETF_crawler_dividend.py        # ETF 配息抓取（彙總同日多筆）
│   ├── ETF_crawler_price.py           # ETF 歷史日價（逐月抓取，連續空月停損）
│   ├── metrics_pipeline.py            # 指標計算 → 物化到 etf_metrics_daily
│   ├── bigquery.py                    # BigQuery 共用工具
│   ├── etf_sync_mysql_to_bigquery.py  # MySQL → BigQuery Raw 同步
│   └── etf_bigquery_transform.py      # 在 BigQuery 建 View / 物化表
│
├── airflow/
│   ├── airflow.cfg
│   ├── Dockerfile
│   ├── docker-compose-airflow.yml
│   └── dags/
│       └── ETF_bigquery_etl_dag.py    # DAG: MySQL → BQ → 轉換
│
├── docker-compose-mysql.yml           # MySQL + phpMyAdmin
├── docker-compose-metabase.yml        # Metabase
├── .env                               # 環境變數（不入版控）
├── pyproject.toml / uv.lock           # 依賴管理（uv）
└── README.md
```

---

## 🔧 先備環境

* Python 3.10+
* Docker Desktop
* uv

建立 Docker 網路（供多服務互通）

```
docker network create my_network
```

安裝依賴

```
uv sync
```

---

## 🌍 環境變數（.env）

程式會讀取 `.env`，缺少時使用預設值。**此檔不要進版控**。

**Docker Hub**（Airflow 自行 build 的 image tag）

```
DOCKER_HUB_USER=<your_dockerhub_username>  # 請改成你自己的 Docker Hub 帳號
```

**MySQL（給你的程式/ETL 使用）**

```
MYSQL_HOST=127.0.0.1   # 本機跑腳本用；Airflow 容器內會覆寫為 mysql
MYSQL_PORT=3306
MYSQL_DB=ETF
MYSQL_USER=app
MYSQL_PASSWORD=
```

**Metabase（應用設定 DB）**

```
MB_DB_USER=metabase
MB_DB_PASS=
```

**Airflow**

```
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASS=
```

**BigQuery / GCP**

```
GCP_PROJECT_ID=etfproject20250923
BQ_DATASET_RAW=etf_raw
BQ_DATASET_ANALYTICS=etf_analytics
GOOGLE_APPLICATION_CREDENTIALS=/home/chris/ETF_DataAnalysis/key.json  # 本機路徑；Airflow 內會掛載到 /opt/airflow/key.json
```

**指標與修正參數**

```
SPLIT_THRESHOLD=0.20  # 拆/合股偵測（非除息日且跳動幅度）
```

> 在**本機**執行 Python 腳本時，`MYSQL_HOST=127.0.0.1`；在 **Airflow 容器** 執行時，DAG 會將 `MYSQL_HOST` 覆寫為 `mysql`（Compose 服務名）。

---

## 🗄️ 啟動服務

**MySQL + phpMyAdmin**

```
docker compose -f docker-compose-mysql.yml up -d
```

* phpMyAdmin: [http://localhost:8000](http://localhost:8000)
* Compose 內已設 `--max_allowed_packet=128M`，避免建立大型 VIEW 時斷線。

**Metabase**

```
docker compose -f docker-compose-metabase.yml up -d
```

* Web: [http://localhost:3000](http://localhost:3000)
* 首次登入後新增 MySQL 連線（與 `.env` 相同）

**Airflow（DAG 排程）**

* build 專用 Airflow image（內含本專案依賴與 dags）

```
docker compose -f airflow/docker-compose-airflow.yml build --no-cache
```

* 啟動

```
docker compose -f airflow/docker-compose-airflow.yml up -d
```

* Web: [http://localhost:8080](http://localhost:8080)
* 首次初始化（若尚未 init）

```
docker compose -f airflow/docker-compose-airflow.yml up -d airflow-init
```

---

## 🚀 本機執行流程（不走 Airflow）

> 建議先載入 `.env`（或每條 `uv run` 後加 `--env-file .env`）

1. 載入環境變數

```
source .env
```

2. 抓 ETF 配息

```
uv run -m data_ingestion.ETF_crawler_dividend
```

3. 抓 ETF 歷史日價

```
uv run -m data_ingestion.ETF_crawler_price
```

4. 計算指標 → 物化 `etf_metrics_daily`

```
uv run -m data_ingestion.metrics_pipeline
```

---

## ☁️ BigQuery：輕量 ELT

本專案採用 **輕量 ELT**：將 MySQL 的原始資料表同步至 `BQ_DATASET_RAW`，並在 `BQ_DATASET_ANALYTICS` 層建立視圖/物化表，供查詢與視覺化使用。

**同步：**把 MySQL 三張表上傳至 `BQ_DATASET_RAW`

* `etf_day_price`（分區：`trade_date`；叢集：`ticker`）
* `etf_dividend`（分區：`ex_date`；叢集：`ticker`）
* `etf_metrics_daily`（分區：`trade_date`；叢集：`ticker`）

**轉換：**在 BigQuery 建 **視圖** 與 **物化表**（供 Metabase / 查詢）

**本機直跑**

```
# 安裝 BQ 相關套件（若未安裝）
uv add google-cloud-bigquery pandas pyarrow pandas-gbq pymysql SQLAlchemy

# 同步 MySQL → BigQuery Raw
uv run --env-file .env -m data_ingestion.etf_sync_mysql_to_bigquery

# 建 view / 物化表（Analytics）
uv run --env-file .env -m data_ingestion.etf_bigquery_transform
```

**用 Airflow 跑（推薦）**

* DAG：`airflow/dags/ETF_bigquery_etl_dag.py`
* 流程：`sync_mysql_to_bigquery → bigquery_transform`
* 在容器內，DAG 會自動設定：

```
MYSQL_HOST=mysql
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/key.json
```

---

## 🗄️ 資料表說明

### `etf_dividend`（唯一鍵：`ticker + ex_date`）

* `ticker, short_name, ex_date, record_date, payable_date`
* `cash_dividend`（小數；同日多筆已彙總）
* `created_at, updated_at`

### `etf_day_price`（唯一鍵：`ticker + trade_date`）

* `volume, amount`（整數）
* `open, high, low, close`（小數）
* `adjusted_close`（由 pipeline 計算）
* `trades, created_at, updated_at`

### `etf_metrics_daily`（唯一鍵：`ticker + trade_date`）

**價格/報酬**

* `adjusted_close` — 還原價（拆/合股、除息、權值調整）
* `daily_return` — 日報酬率 `(adj_t/adj_{t-1})-1`
* `tri_total_return` — 成立以來累積報酬（`Π(1+daily_return)-1`）

**風險/回撤**

* `vol_252` — 年化波動度 `std(daily_return_252)*sqrt(252)`
* `sharpe_252d` — 年化夏普值 `mean/std * sqrt(252)`（`RF=0`）
* `drawdown` — 回撤 `adj_t/max(adj_≤t)-1`
* `mdd` — 最大回撤 `min(drawdown_≤t)`

**股利/殖利率**

* `dividend_12m` — 近 12 個月現金股利總額
* `dividend_yield_12m` — 近 12 個月殖利率 `dividend_12m/adjusted_close`

---

## 🆕 指標

> GitHub 版面相容：每個指標用**小標題 + 清單 + 獨立 code block**，避免擠成同一行。

### 1) 總報酬率（Total Return） `total_return`

* 用途：衡量整段期間的整體漲跌幅。
* 公式（可讀）：

```
(期末資產 ÷ 期初資產) − 1
```

* 等價寫法（複利）：

```
∏(1 + r_t) − 1
```

---

### 2) 年化報酬率（CAGR） `cagr`

* 用途：把整段報酬換算成每年的穩定成長率，便於不同區間/產品比較。
* 公式：

```
CAGR = (期末資產 ÷ 期初資產)^(365/實際天數) − 1
```

* 等價式：

```
CAGR = (1 + total_return)^(365/D) − 1
```

---

### 3) 最大回撤（Max Drawdown） `max_drawdown`

* 用途：評估「最壞情況會跌多深」。
* 公式：

```
(谷底 − 高峰) ÷ 高峰
# 限制：谷底必須發生在高峰之後；整段期間取最小值
```

---

### 4) 年化波動率（Annualized Volatility） `vol_ann`

* 用途：衡量價格/報酬的波動程度，是 Sharpe 比的分母。
* 公式：

```
σ_年 = σ_日 × √252
# σ_日 為期間內日報酬的標準差
```

---

### 5) 夏普值（Sharpe Ratio） `sharpe_ratio`

* 用途：每承擔 1 單位波動風險可得到多少報酬；越高越好。
* 公式：

```
Sharpe = 年化報酬 ÷ 年化波動
# 本專案假設無風險利率 RF = 0
```

---

### 6) 近 12 個月殖利率平均 `div_yield_12m_avg`

* 用途：觀察期間內的股息收益水準。
* 公式：

```
平均( 近12個月現金股利 ÷ 當日價格 )
# 對期間取平均
```

---

### 7) 近 12 個月現金股利（最新） `dividend_12m_latest`

* 用途：呈現期間結束當天的 12 個月累計現金股利（TTM）。
* 公式：

```
取「期末日」的 dividend_12m；若期末為空，取最近一筆非空值
```

---

## 📊 Metabase 面板卡片

**全域篩選器**

* `ticker`（多選）
* `date range`（以 `trade_date`）

**卡片 A：月平均累積報酬率（折線）**

* 目的：對多檔 ETF 的累積報酬（以月尺度觀察）進行橫向比較。
* 維度：`trade_date`（By Month）
* 度量：`total_return`（或以 View 產生之月度累積欄位）
* 分組：`ticker` 作為多序列。
* 呈現：Y 軸顯示為百分比。

**卡片 B：ETF 概覽（表格）**

* 目的：提供區間 KPI 的一覽與排序。
* 欄位：`ticker`, `period_start`, `period_end`, `total_return`, `cagr`, `max_drawdown`, `vol_ann`, `sharpe_ratio`, `div_yield_12m_avg`, `dividend_12m_latest`
* 互動：依 `total_return` 由高到低排序；以 `ticker`、`date range` 篩選動態刷新 KPI。

---

## 🛠️ 疑難排解

**MySQL 斷線 / 大結果集**
Compose 已設 `--max_allowed_packet=128M`；必要時重建：

```
docker compose -f docker-compose-mysql.yml up -d --force-recreate
```

**Airflow DAG 一直失敗**

* 檢查 Scheduler 容器內環境：

```
docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '
  grep -E "GCP_PROJECT_ID|GOOGLE_APPLICATION_CREDENTIALS|MYSQL_HOST" /opt/airflow/.env || true
  getent hosts mysql || true
  ls -l /opt/airflow/key.json || true
  python3 -c "import google.cloud.bigquery,pyarrow,pandas,pymysql,sqlalchemy;print(\"deps OK\")"
'
```

* 手動在容器內跑腳本看錯誤：

```
docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '
  set -a; source /opt/airflow/.env; set +a
  export PYTHONPATH=/opt/airflow
  python3 -m data_ingestion.etf_sync_mysql_to_bigquery
  python3 -m data_ingestion.etf_bigquery_transform
'
```

**BigQuery 權限錯誤**
確認 `.env` 的金鑰路徑與容器內 `/opt/airflow/key.json` 一致，並且 SA 具備 BigQuery Admin / Job User / Storage Viewer 等必要權限。

---

## 📦 版本控與提交

提醒：把 `key.json`、`.env` 加進 `.gitignore`，不要 push 憑證。

```
git add .
git commit -m "docs: 精簡求職版 README；修正排版＆加上可讀公式"
git push
```

---

## 📚 附錄：常用指令速查

**套件**

```
uv sync
uv add google-cloud-bigquery pandas pyarrow pandas-gbq pymysql SQLAlchemy
```

**本機執行**

```
source .env
uv run -m data_ingestion.ETF_crawler_dividend
uv run -m data_ingestion.ETF_crawler_price
uv run -m data_ingestion.metrics_pipeline
uv run -m data_ingestion.etf_sync_mysql_to_bigquery
uv run -m data_ingestion.etf_bigquery_transform
```

**Docker**

```
docker network create my_network
# DB & UI
docker compose -f docker-compose-mysql.yml up -d
docker compose -f docker-compose-metabase.yml up -d
# Airflow
docker compose -f airflow/docker-compose-airflow.yml build --no-cache
docker compose -f airflow/docker-compose-airflow.yml up -d
```

---

## About

ETF 高股息型與市值型分析專案（教育性質）
