# 市值型 vs 高股息型 ETF

以 **台股 ETF** 為核心的資料工程專案：從資料擷取、指標計算到 **BigQuery / Metabase** 視覺化，並以 **Apache Airflow** 編排整體流程。專案設計重視「**本機先跑通、再上雲**」的學習曲線，本版 README 加入市值型與高股息型 ETF 比較分析、可讀的指標公式與 Metabase 面板說明。

---

## 🏗️ 架構總覽

* **資料來源**：**臺灣證券交易所 (TWSE)** API（ETF 歷史日價、配息）
* **運算編排**：Apache Airflow（每日排程）
* **資料庫**：MySQL 8（搭配 phpMyAdmin）
* **資料倉儲**：Google BigQuery（Raw + Analytics）
* **視覺化**：Metabase（儀表板 + Field Filters）
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

### 分析目標

* 比較「市值型」與「高股息型」ETF 的區間績效與風險（Total Return、CAGR、MDD、Vol、Sharpe、TTM 殖利率）。
* 支援可調整的 **ticker** 與 **日期區間** 篩選，觀察不同期間的表現差異。
* 提供回測與技術指標分析，支援投資決策與策略研究。

---

## 🧱 系統功能模組

### 1️⃣ ETF 資料蒐集爬蟲

* **台股 ETF 清單**：透過 **臺灣證券交易所 (TWSE)** API 取得全部上市 ETF 代碼與基本資料。
* **歷史價格下載**：逐檔抓取 2020‑01‑01 起至今的每日歷史價格：`open, high, low, close, volume, adjusted_close`（調整後收盤價考慮配息與權值調整）。
* **配息資料下載**：擷取 `ex_date`（除息日）、`cash_dividend`（每單位現金股利）。同日多筆會彙總。
* **資料存放**：原始資料寫入 MySQL。所有寫入採 **idempotent UPSERT**（唯一鍵避免重覆/髒資料）：

  * `etf_day_price`（唯一鍵：`ticker + trade_date`）
  * `etf_dividend`（唯一鍵：`ticker + ex_date`）
  * `etf_metrics_daily`（唯一鍵：`ticker + trade_date`）

> 可靠性：爬蟲具 **重試、斷點續跑、逐月抓取 + 連續空月停損** 等機制。

#### 🐬 MySQL（資料落地與一致性）

* 資料庫：`ETF`（Docker Compose 內建 MySQL 服務與 phpMyAdmin）
* 連線：本機腳本 `127.0.0.1:3306`；容器/ Airflow 內 `mysql:3306`
* 唯一鍵（避免重覆/髒資料）：

  * `etf_day_price (ticker, trade_date)`
  * `etf_dividend (ticker, ex_date)`
  * `etf_metrics_daily (ticker, trade_date)`
* UPSERT 範例（pymysql/SQLAlchemy 皆可複製）：

```sql
INSERT INTO etf_day_price (ticker, trade_date, open, high, low, close, volume, adjusted_close, trades)
VALUES (:ticker, :trade_date, :open, :high, :low, :close, :volume, :adjusted_close, :trades)
ON DUPLICATE KEY UPDATE
  open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
  volume=VALUES(volume), adjusted_close=VALUES(adjusted_close), trades=VALUES(trades),
  updated_at=NOW();
```

* 連線字串：

```text
# 本機（腳本/Notebook）
mysql+pymysql://app:${MYSQL_PASSWORD}@127.0.0.1:3306/ETF?charset=utf8mb4
# 容器/ Airflow 內
mysql+pymysql://app:${MYSQL_PASSWORD}@mysql:3306/ETF?charset=utf8mb4
```

### 2️⃣ 指標計算與資料管線（`metrics_pipeline.py`）

* 以 **還原價 `adjusted_close`** 計算 `daily_return`；
* 根據 `daily_return` 推導：`total_return`、`cagr`、`max_drawdown`、`vol_ann`、`sharpe_ratio`、`div_yield_12m_avg`、`dividend_12m_latest`（取期末或最近一筆）。
* 產出物化表 `etf_metrics_daily`（每天一筆、可追溯）。

### 3️⃣ 雲端同步與轉換（BigQuery）

* **同步**：`etf_sync_mysql_to_bigquery.py` → 將三張 MySQL 表同步到 **BQ RAW**。
* **轉換**：`etf_bigquery_transform.py` → 在 **BQ Analytics** 建立 **視圖/物化表**（供 KPI 聚合、月/週彙總）。

### 4️⃣ DAG 排程（Airflow）

* `airflow/dags/ETF_bigquery_etl_dag.py`：每日定時執行 **sync → transform**，具備重試與可追蹤性（可 backfill）。

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

建立 Docker 網路（供多服務互通）：

```bash
docker network create my_network
```

安裝依賴：

```bash
uv sync
```

---

## 🌍 環境變數（.env）

> 程式會讀取 `.env`，缺少時使用預設值。**此檔不要進版控**。

**Docker Hub**（Airflow 自行 build 的 image tag）

```dotenv
DOCKER_HUB_USER=<your_dockerhub_username>
```

**MySQL（給程式/ETL 使用）**

```dotenv
MYSQL_HOST=127.0.0.1   # 本機跑腳本用；Airflow 容器內會覆寫為 mysql
MYSQL_PORT=3306
MYSQL_DB=ETF
MYSQL_USER=app
MYSQL_PASSWORD=
```

**Metabase（應用設定 DB）**

```dotenv
MB_DB_USER=metabase
MB_DB_PASS=
```

**Airflow**

```dotenv
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASS=
```

**BigQuery / GCP**

```dotenv
GCP_PROJECT_ID=etfproject20250923
BQ_DATASET_RAW=etf_raw
BQ_DATASET_ANALYTICS=etf_analytics
GOOGLE_APPLICATION_CREDENTIALS=/home/chris/ETF_DataAnalysis/key.json  # 本機；容器內掛 /opt/airflow/key.json
```

**指標與修正參數**

```dotenv
SPLIT_THRESHOLD=0.20  # 拆/合股偵測（非除息日且跳動幅度）
```

> 在 **本機** 執行 Python 腳本時使用 `MYSQL_HOST=127.0.0.1`；在 **Airflow 容器** 執行時，DAG 會覆寫為 `mysql`（Compose 服務名）。

---

## 🗄️ 啟動服務（Docker Compose）

**MySQL + phpMyAdmin**

```bash
docker compose -f docker-compose-mysql.yml up -d
# phpMyAdmin: http://localhost:8000
```

**Metabase**

```bash
docker compose -f docker-compose-metabase.yml up -d
# Web: http://localhost:3000
```

**Airflow（DAG 排程）**

```bash
# build 專用 image（內含依賴與 dags）
docker compose -f airflow/docker-compose-airflow.yml build --no-cache
# 啟動
...
docker compose -f airflow/docker-compose-airflow.yml up -d
# 首次初始化（若尚未 init）
docker compose -f airflow/docker-compose-airflow.yml up -d airflow-init
# Web: http://localhost:8080
```

---

## 🐬 MySQL（連線與管理）

**服務位置**

* 本機腳本連線主機：`127.0.0.1`（來自 `.env` 的 `MYSQL_HOST`）
* 容器／Airflow 內連線主機：`mysql`（Compose 服務名）
* 連接埠：`3306`
* 管理介面（phpMyAdmin）：`http://localhost:8000`

**應用連線字串**（擇一）：

```text
# Python SQLAlchemy / pandas-gbq 以外的 MySQL 連線（pymysql 驅動）
mysql+pymysql://app:${MYSQL_PASSWORD}@127.0.0.1:3306/ETF?charset=utf8mb4

# 容器內（例如 Airflow DAG 執行時）
mysql+pymysql://app:${MYSQL_PASSWORD}@mysql:3306/ETF?charset=utf8mb4
```

**資料表唯一鍵（避免重覆/髒資料）**

* `etf_day_price`：`(ticker, trade_date)`
* `etf_dividend`：`(ticker, ex_date)`
* `etf_metrics_daily`：`(ticker, trade_date)`

**快速檢查**

```bash
# 進入 MySQL 容器並開啟 CLI
docker compose -f docker-compose-mysql.yml exec mysql bash -lc 'mysql -u app -p$MYSQL_PASSWORD ETF -e "SHOW TABLES;"'

# 檢查每日價量筆數
docker compose -f docker-compose-mysql.yml exec mysql bash -lc \
  'mysql -u app -p$MYSQL_PASSWORD ETF -e "SELECT ticker, COUNT(*) cnt FROM etf_day_price GROUP BY ticker ORDER BY cnt DESC LIMIT 10;"'
```

---

## 🚀 本機執行流程（不走 Airflow）

> 建議先載入 `.env`（或每條 `uv run` 後加 `--env-file .env`）

```bash
# 1) 載入環境變數
source .env

# 2) 抓 ETF 配息
uv run -m data_ingestion.ETF_crawler_dividend

# 3) 抓 ETF 歷史日價
uv run -m data_ingestion.ETF_crawler_price

# 4) 計算指標 → 物化 etf_metrics_daily
uv run -m data_ingestion.metrics_pipeline
```

---

## ☁️ BigQuery：輕量 ELT

**同步到 RAW**

* `etf_day_price`（分區：`trade_date`；叢集：`ticker`）
* `etf_dividend`（分區：`ex_date`；叢集：`ticker`）
* `etf_metrics_daily`（分區：`trade_date`；叢集：`ticker`）

**轉換到 Analytics**

* 建 **視圖** / **物化表** 供 Metabase / SQL 查詢。

**以 Airflow 執行（推薦）**

* DAG：`airflow/dags/ETF_bigquery_etl_dag.py`
* 流程：`sync_mysql_to_bigquery → bigquery_transform`
* 容器內自動設定：`MYSQL_HOST=mysql`、`GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/key.json`

---

## 🗄️ 資料表說明

### `etf_dividend`（唯一鍵：`ticker + ex_date`）

* 欄位：`ticker, short_name, ex_date, record_date, payable_date, cash_dividend, created_at, updated_at`

### `etf_day_price`（唯一鍵：`ticker + trade_date`）

* 欄位：`volume, amount, open, high, low, close, adjusted_close, trades, created_at, updated_at`

### `etf_metrics_daily`（唯一鍵：`ticker + trade_date`）

* **價格/報酬**：`adjusted_close`、`daily_return`、`tri_total_return`
* **風險/回撤**：`vol_252`、`sharpe_252d`、`drawdown`、`mdd`
* **股利/殖利率**：`dividend_12m`、`dividend_yield_12m`

---

## 🆕 指標（可讀公式 + 用途）

### 1) 總報酬率（Total Return）

* 用途：衡量整段期間的整體漲跌幅。
* 公式（可讀）：`(期末資產 ÷ 期初資產) − 1` ；等價於 `∏(1 + r_t) − 1`（以日報酬 `r_t` 連乘）。

### 2) 年化報酬率（Compound Annual Growth Rate:CAGR）

* 用途：把整段報酬換算成每年的穩定成長率，便於不同區間/產品比較。
* 公式：`CAGR = (期末 ÷ 期初)^(365/實際天數) − 1` ；等價於 `CAGR = (1 + total_return)^(365/D) − 1`。

### 3) 最大回撤（Max Drawdown）

* 用途：評估「最壞情況會跌多深」。
* 公式：`(谷底 − 高峰) ÷ 高峰`（谷底須發生在高峰之後；期間內取最小值）。

### 4) 年化波動率（Annualized Volatility）

* 用途：衡量價格/報酬的波動程度，是 Sharpe 的分母。
* 公式：`σ_年 = σ_日 × √252`（`σ_日` 為期間內日報酬的標準差）。

### 5) 夏普值（Sharpe Ratio）

* 用途：每承擔 1 單位波動風險可得到多少報酬，越高越好。
* 公式：`Sharpe = 年化報酬 ÷ 年化波動`（本專案假設無風險利率 `RF=0`）。

### 6) 殖利率－近 12 個月平均（Dividend Yield, avg 12M）

* 用途：觀察期間內的股息收益水準。
* 公式：`平均(近12個月現金股利 ÷ 當日價格)`，對區間 `P` 取平均。

---

## 📊 Metabase 面板卡片

**全域篩選器**：`ticker`（多選）、`date range`（以 `trade_date`）

### 卡片 A：月平均累積報酬率（折線圖）

* 目的：對多檔 ETF 的累積報酬（以月尺度）做橫向比較。
* 維度：`trade_date`（按月分組）。
* 度量：`total_return`（或月度累積欄位）。
* 分組：`ticker` 多序列；Y 軸以百分比顯示。

### 卡片 B：ETF 概覽（表格）

* 目的：區間 KPI 一覽與排序，快速比較市值型 vs 高股息型表現。
* 欄位（依儀表板實際順序）：`ETF代碼`, `期間起`, `期間迄`, `最大回撤 (MDD)`, `年化波動率`, `夏普值`, `殖利率 (近12月)`, `總報酬率`, `年化報酬率 (CAGR)`。
* 互動：依 `總報酬率`、`CAGR`、`MDD` 等欄位排序；支援 ticker / date range 篩選。

> BigQuery / Metabase Field Filter 版本的彙總查詢（log‑sum 複利、實際天數年化）已內建於專案 SQL 中，可直接套用。

---

## 📎 附錄：分析 ETF 清單（市值型 vs 高股息型）

| 類型   | ETF 代碼 | ETF 名稱     |
| ---- | ------ | ---------- |
| 市值型  | 0050   | 元大台灣50     |
| 市值型  | 00881  | 國泰台灣科技龍頭   |
| 市值型  | 00850  | 元大台灣ESG永續  |
| 市值型  | 00922  | 國泰台灣領袖50   |
| 市值型  | 00692  | 富邦公司治理     |
| 高股息型 | 0056   | 元大高股息      |
| 高股息型 | 00713  | 元大台灣高息低波   |
| 高股息型 | 00878  | 國泰永續高股息    |
| 高股息型 | 00919  | 群益台灣精選高息   |
| 高股息型 | 00929  | 復華台灣科技高息30 |

> 選股邏輯：市值型與高股息型各取基金規模前 5 名（排除與 0050 同指數之 006208）。

---

## 🛠️ 疑難排解

**MySQL 斷線 / 大結果集**：Compose 已設 `--max_allowed_packet=128M`，必要時重建：

```bash
docker compose -f docker-compose-mysql.yml up -d --force-recreate
```

**Airflow DAG 一直失敗**：檢查 Scheduler 容器內環境：

```bash
docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '
  grep -E "GCP_PROJECT_ID|GOOGLE_APPLICATION_CREDENTIALS|MYSQL_HOST" /opt/airflow/.env || true
  getent hosts mysql || true
  ls -l /opt/airflow/key.json || true
  python3 -c "import google.cloud.bigquery,pyarrow,pandas,pymysql,sqlalchemy;print(\"deps OK\")"
'
```

手動在容器內跑同步/轉換：

```bash
docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '
  set -a; source /opt/airflow/.env; set +a
  export PYTHONPATH=/opt/airflow
  python3 -m data_ingestion.etf_sync_mysql_to_bigquery
  python3 -m data_ingestion.etf_bigquery_transform
'
```

**BigQuery 權限錯誤**：確認 `.env` 金鑰路徑與容器內 `/opt/airflow/key.json` 一致，並確保 SA 具備 BigQuery Admin / Job User / Storage Viewer 權限。

---

## 📦 版本控與提交

> 把 `key.json`、`.env` 加進 `.gitignore`，不要 push 憑證。

```bash
git add .
git commit -m "docs: 求職版 README；指標公式、Metabase 卡片、ETF 清單"
git push
```

---

## 📚 附錄：常用指令速查

**套件**

```bash
uv sync
uv add google-cloud-bigquery pandas pyarrow pandas-gbq pymysql SQLAlchemy
```

**本機執行**

```bash
source .env
uv run -m data_ingestion.ETF_crawler_dividend
uv run -m data_ingestion.ETF_crawler_price
uv run -m data_ingestion.metrics_pipeline
uv run -m data_ingestion.etf_sync_mysql_to_bigquery
uv run -m data_ingestion.etf_bigquery_transform
```

**Docker**

```bash
docker network create my_network
# DB & UI
docker compose -f docker-compose-mysql.yml up -d
docker compose -f docker-compose-metabase.yml up -d
# Airflow
docker compose -f airflow/docker-compose-airflow.yml build --no-cache
docker compose -f airflow/docker-compose-airflow.yml up -d
```
