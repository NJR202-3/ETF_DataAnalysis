# 市值型 vs 高股息型 ETF

以 **台股 ETF** 為核心的資料工程專案：從資料擷取、指標計算到 **BigQuery / Metabase** 視覺化，並以 **Apache Airflow** 編排整體流程。專案設計重視「**本機先跑通、再上雲**」的學習曲線。

---

## 🏗️ 架構總覽

* **資料來源**：**臺灣證券交易所 (TWSE)** API（ETF 歷史日價、配息）
* **運算編排**：Apache Airflow（每日排程）
* **資料庫**：MySQL 8（搭配 phpMyAdmin）
* **資料倉儲**：Google BigQuery（Raw + Analytics）
* **視覺化**：Metabase（儀表板 + Field Filters）
* **容器化**：Docker & Docker Compose
* **套件管理**：uv（更快的 Python 套件管理）

### 資料流程（本機先跑通、再上雲）

```
           ┌──────────────────────┐
           │  臺灣證券交易所 (TWSE) │
           └──────────┬───────────┘
                      │ (API)
                      ▼
                Python 爬蟲
                      │
                      ▼
                    MySQL  (RAW: etf_day_price / etf_dividend)
                      │
                      ▼
             metrics_pipeline.py  (計算指標 → 物化到 MySQL: etf_metrics_daily)
                      │
                      ▼
           (BigQuery ELT 同步與轉換)
                      ▼
                BigQuery RAW (三表)
                      │
                      ▼
             BigQuery Analytics (視圖/物化表)
                      │
                      ▼
                   Metabase 儀表板

Airflow DAG：編排整段流程（抓取 → 計算 → 同步 → 轉換），支援重試 / backfill。
```

### 分析目標

* 比較「市值型」與「高股息型」ETF 的區間績效與風險（**含息總報酬 TRI**、CAGR、MDD、年化波動、Sharpe、近 12 個月殖利率）。
* 支援可調整的 **ticker** 與 **日期區間** 篩選，觀察不同期間的表現差異。
* 提供回測與技術指標分析，支援投資決策與策略研究。

---

## 🧱 系統功能模組

### 1️⃣ ETF 資料蒐集爬蟲

* **台股 ETF 清單**：透過 **臺灣證券交易所 (TWSE)** API 取得全部上市 ETF 代碼與基本資料。
* **歷史價格下載**：逐檔抓取每日歷史價格：`open, high, low, close, volume`。
* **配息資料下載**：擷取 `ex_date`（除息日）、`cash_dividend`（每單位現金股利）。同日多筆會彙總。
* **資料存放**：原始資料寫入 MySQL。所有寫入採 **idempotent UPSERT**（唯一鍵避免重覆/髒資料）：

  * `etf_day_price`（唯一鍵：`ticker + trade_date`）
  * `etf_dividend`（唯一鍵：`ticker + ex_date`）
  * `etf_metrics_daily`（唯一鍵：`ticker + trade_date`）

> 可靠性：爬蟲具 **重試、斷點續跑、逐月抓取 + 連續空月停損** 等機制。

#### 🐬 MySQL（資料落地與一致性）

* 資料庫：`ETF`（Docker Compose 內建 MySQL 服務與 phpMyAdmin）
* 連線：本機腳本 `127.0.0.1:3306`；容器/Airflow 內 `mysql:3306`
* UPSERT 範例：

```sql
INSERT INTO etf_day_price (ticker, trade_date, open, high, low, close, volume, adjusted_close, trades)
VALUES (:ticker, :trade_date, :open, :high, :low, :close, :volume, :adjusted_close, :trades)
ON DUPLICATE KEY UPDATE
  open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close),
  volume=VALUES(volume), adjusted_close=VALUES(adjusted_close), trades=VALUES(trades),
  updated_at=NOW();
```

---

### 2️⃣ 指標計算與資料管線（`metrics_pipeline.py`）

> **關鍵修正：TRI（含息再投資）已內建**

* 以 **`adjusted_close` 只做拆/合股校正**（不把現金股利併入價格），避免誤把股息視為價格調整。
* 兩種日報酬：

  * `daily_return`（px）：**純價格**日報酬 `(P_t/P_{t-1}-1)`。
  * `daily_return_tri`：**含息**日報酬 `((P_t + D_t)/P_{t-1} - 1)`，`D_t` 為當天現金股利（非除息日 = 0）。
* 逐日複利：

  * `total_return`（px）：`∏(1+r_px) − 1`
  * `tri_total_return`：`∏(1+r_TRI) − 1`（**用於面板的總報酬**）
* 其它指標：`vol_252`、`sharpe_252d`、`drawdown`、`mdd`、`dividend_12m`、`dividend_yield_12m`。
* 物化：寫入 MySQL 表 `etf_metrics_daily`，每天一筆、可追溯。

---

### 3️⃣ 雲端同步與轉換（BigQuery）

* **同步**：`etf_sync_mysql_to_bigquery.py` → 將三張表同步到 **BQ RAW**。
* **轉換**：`etf_bigquery_transform.py` → 在 **BQ Analytics** 建立：

  * `vw_metrics_daily`（投影 RAW 的指標欄位）
  * `metrics_latest`（各 ETF 最新一日快照，供首頁/健康檢查）

---

### 4️⃣ DAG 排程（Airflow）

* **`airflow/dags/ETF_crawler_etl_dag.py`**：`ETF 爬蟲 → metrics_pipeline → 寫入 MySQL`（每日排程、可 backfill）
* **`airflow/dags/ETF_bigquery_etl_dag.py`**：`MySQL → BigQuery RAW → Analytics 轉換`（每日排程、可 backfill）
* Web UI：`http://localhost:8080`
* 初始化：

```bash
docker compose -f airflow/docker-compose-airflow.yml build --no-cache
docker compose -f airflow/docker-compose-airflow.yml up -d
docker compose -f airflow/docker-compose-airflow.yml up -d airflow-init
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
│   ├── metrics_pipeline.py            # 指標計算（TRI/px）→ 物化 etf_metrics_daily
│   ├── bigquery.py                    # BigQuery 共用工具
│   ├── etf_sync_mysql_to_bigquery.py  # MySQL → BigQuery RAW 同步
│   └── etf_bigquery_transform.py      # 在 BigQuery 建 View / 物化表
│
├── airflow/
│   ├── airflow.cfg
│   ├── Dockerfile
│   ├── docker-compose-airflow.yml
│   └── dags/
│       ├── ETF_crawler_etl_dag.py     # DAG: 爬蟲 → 指標 → MySQL（20:00）
│       └── ETF_bigquery_etl_dag.py    # DAG: MySQL → BQ → 轉換（20:30）
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

```dotenv
# MySQL（給程式/ETL 使用）
MYSQL_HOST=127.0.0.1   # 本機跑腳本用；Airflow 容器內會覆寫為 mysql
MYSQL_PORT=3306
MYSQL_DB=ETF
MYSQL_USER=app
MYSQL_PASSWORD=

# BigQuery / GCP
GCP_PROJECT_ID=etfproject20250923
BQ_DATASET_RAW=etf_raw
BQ_DATASET_ANALYTICS=etf_analytics
GOOGLE_APPLICATION_CREDENTIALS=/home/you/ETF_DataAnalysis/key.json  # 本機；容器內掛 /opt/airflow/key.json

# Metabase
MB_DB_USER=metabase
MB_DB_PASS=

# Airflow
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASS=

# 指標與修正參數（非除息 & 跳動過大 => 拆/合股）
SPLIT_THRESHOLD=0.20
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

* 本機腳本主機：`127.0.0.1`；容器／Airflow 主機：`mysql`；連接埠：`3306`
* 應用連線字串：

```text
mysql+pymysql://app:${MYSQL_PASSWORD}@127.0.0.1:3306/ETF?charset=utf8mb4
# 容器內
mysql+pymysql://app:${MYSQL_PASSWORD}@mysql:3306/ETF?charset=utf8mb4
```

* 資料表唯一鍵：

  * `etf_day_price (ticker, trade_date)`
  * `etf_dividend  (ticker, ex_date)`
  * `etf_metrics_daily (ticker, trade_date)`

---

## 🚀 本機執行流程（不走 Airflow）

```bash
# 1) 載入環境變數
source .env

# 2) 抓 ETF 配息
uv run -m data_ingestion.ETF_crawler_dividend

# 3) 抓 ETF 歷史日價
uv run -m data_ingestion.ETF_crawler_price

# 4) 計算指標 → 物化 etf_metrics_daily（含 TRI）
uv run -m data_ingestion.metrics_pipeline
```

---

## ☁️ BigQuery：輕量 ELT

**同步到 RAW**

* `etf_day_price`（分區：`trade_date`；叢集：`ticker`）
* `etf_dividend`（分區：`ex_date`；叢集：`ticker`）
* `etf_metrics_daily`（分區：`trade_date`；叢集：`ticker`）

**轉換到 Analytics**

* 建 **視圖**：`vw_metrics_daily`
* 建 **物化表**：`metrics_latest`（各 ETF 期末快照）

**以 Airflow 執行（推薦）**

* DAG：`ETF_bigquery_etl_dag`
* 流程：`sync_mysql_to_bigquery → bigquery_transform`
* 容器內自動設定：`MYSQL_HOST=mysql`、`GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/key.json`

---

## 🗄️ 資料表說明

### `etf_dividend`（唯一鍵：`ticker + ex_date`）

* 欄位：`ticker, short_name, ex_date, record_date, payable_date, cash_dividend, created_at, updated_at`

### `etf_day_price`（唯一鍵：`ticker + trade_date`）

* 欄位：`volume, amount, open, high, low, close, adjusted_close, trades, created_at, updated_at`

### `etf_metrics_daily`（唯一鍵：`ticker + trade_date`）

* **價格/報酬**：`adjusted_close`、`daily_return`（px） 、`daily_return_tri`（TRI） 、`tri_total_return`、`total_return`（px）
* **風險/回撤**：`vol_252`、`sharpe_252d`、`drawdown`、`mdd`
* **股利/殖利率**：`dividend_12m`、`dividend_yield_12m`

---

## 🆕 指標（可讀公式 + 用途）

1. **總報酬率（Total Return）**

   * 可讀公式：`(期末 ÷ 期初) − 1`；等價於複利 `∏(1 + r_t) − 1`。
   * 本專案預設採 **TRI（含息）** 總報酬：使用 `daily_return_tri` 連乘。

2. **年化報酬率（CAGR）**

   * 公式：`CAGR = (1 + total_return)^(365/實際天數) − 1`。
   * 面板範例見下（使用 TRI）。

3. **最大回撤（MDD）**

   * 公式：`(谷底 − 高峰) ÷ 高峰`（谷底須發生在高峰之後）。

4. **年化波動率（Annualized Volatility）**

   * 公式：`σ_年 = σ_日 × √252`（以 252 交易日）。

5. **夏普值（Sharpe Ratio）**

   * 公式：`Sharpe = 年化報酬 ÷ 年化波動`（假設 `RF=0`）。

6. **殖利率－近 12 個月平均（Dividend Yield, avg 12M）**

   * 公式：`平均(近12個月現金股利 ÷ 當日價格)`。

---

## 📊 Metabase 面板卡片（使用 TRI）

**全域篩選器**：`ticker`（多選）、`date range`（以 `trade_date`）

* **Total Return（含息）**：

  ```text
  max([Tri Total Return])
  ```

* **CAGR（含息）**：

  ```text
  case(
    datetimeDiff([Min of Trade Date: Day], [Max of Trade Date: Day], "day") = 0, 0,
    power(1 + max([Tri Total Return]),
          365 / datetimeDiff([Min of Trade Date: Day], [Max of Trade Date: Day], "day")
    ) - 1
  )
  ```

* **月平均累積報酬率折線圖**：以 `trade_date`（按月分組） + `max(Tri Total Return)`（或使用物化的月末欄位）作多序列比較。

> 提示：請避免在 UI 端再用 `log(1+R)` 重算 TRI；直接使用我們物化好的 `Tri Total Return` 會更準確與穩定。

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

## 🛠️ 疑難排解（精簡）

* **MySQL 斷線 / 大結果集**：Compose 已設 `--max_allowed_packet=128M`，必要時重建：

  ```bash
  docker compose -f docker-compose-mysql.yml up -d --force-recreate
  ```
* **Airflow DAG 失敗**：檢查 Scheduler 內環境變數與 SA 金鑰掛載路徑（`/opt/airflow/key.json`）。
* **BigQuery 權限**：Service Account 至少需 `BigQuery Admin / Job User / Storage Viewer`。

---

## 📦 版本控與提交

把 `key.json`、`.env` 加進 `.gitignore`，不要 push 憑證。

```bash
git add .
git commit -m "docs: README (TRI 版)；指標與 Metabase 公式更新"
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
