ETF_DataAnalysis

以 台股 ETF 為核心的資料工程專案：從資料擷取、指標計算到 BigQuery / Metabase 視覺化，並以 Apache Airflow 編排整體流程。專案設計重視「本機先跑通、再上雲」的學習曲線，本版 README 加入了市值型與高股息型 ETF 比較分析的應用範例。

🏗️ 架構總覽

資料來源：臺灣證券交易所 (TWSE) API – 提供 ETF 歷史日價與配息資料。

運算編排：Apache Airflow（每日排程）。

資料庫：MySQL 8（搭配 phpMyAdmin）。

資料倉儲：Google BigQuery（分 Raw 層與 Analytics 層）。

視覺化：Metabase（交互式儀表板）。

容器化：Docker & Docker Compose。

套件管理：uv – 加速 Python 依賴管理。

資料流程
TWSE → Python 爬蟲 → MySQL → metrics_pipeline
                ↓
        (BigQuery ELT 同步與轉換)
                ↓
             Metabase
        ↑                 ↓
      Airflow DAG   Analytics views / tables

分析目標

比較市值型 vs 高股息型 ETF：針對市值型與高股息型 ETF，各挑選基金規模 (AUM) 最大前 5 檔（排除與 0050 指數重複的 006208），比較其長期投資表現。

建立每日自動化資料更新管線，涵蓋爬蟲、指標計算、資料同步與視覺化。

提供回測與技術指標分析，支援投資決策與策略研究。

🧱 系統功能模組
1️⃣ ETF 資料蒐集爬蟲

為支撐後續的分析回測，本專案首先構建了自動化爬蟲來收集台股 ETF 的資料。爬蟲採用分步驟撰寫、模組化設計，方便日後擴充到其他市場。

📌 ETF 清單：透過臺灣證券交易所 (TWSE) 公開 API 取得全部上市 ETF 代碼與基本資料，並依基金規模 (AUM) 挑選市值型與高股息型各五檔（排除與 0050 指數重複的 006208）。

📈 歷史日價下載：對每檔 ETF 呼叫 TWSE API，自 2015‑01‑01 起抓取每日開盤價、最高價、最低價、收盤價、成交量與調整後收盤價（還原價），確保日報酬計算連續可比。

💰 配息資料下載：擷取各檔 ETF 的除息日 (ex_date) 與現金股利 (cash_dividend)，對同一天多筆配息做彙總，輸出至 etf_dividend 表。

🗃️ 資料寫入與清洗：抓取後的原始資料寫入 MySQL，利用 mysql.py 中的 UPSERT 規則去除重覆/髒資料；後續由 Airflow 排程執行指標計算與 BigQuery 同步。

爬蟲腳本位於 data_ingestion/ETF_crawler_price.py 與 data_ingestion/ETF_crawler_dividend.py。如需擴充其他市場，只需替換資料來源 URL 及欄位對映即可。

2️⃣ 指標計算與資料管線

爬蟲抓取原始資料後，需將其轉換為可用的分析指標並同步至雲端倉儲。該流程分為三個模組：

🧮 指標計算 (metrics_pipeline.py)：根據日資料先計算日報酬 (daily_return)；再依序推導出 total_return、cagr、max_drawdown、vol_ann、sharpe_ratio、div_yield_12m_avg、dividend_12m_latest 等指標。所有公式皆可複製為 SQL/BI 計算。

🔄 資料同步與轉換：使用 etf_sync_mysql_to_bigquery.py 將 MySQL 表同步至 BigQuery RAW 層，然後透過 etf_bigquery_transform.py 建立 Analytics 視圖與物化表，支援日/週/月聚合與 KPI 統計。

🗓️ 排程執行 (Airflow DAG)：airflow/dags/ETF_bigquery_etl_dag.py 定義了每日排程的 DAG；它會串聯爬蟲、同步與轉換作業，使資料每日自動更新且可追溯。

📁 專案結構
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

🔧 先備環境

Python 3.10 以上

Docker Desktop

uv

建立 Docker 網路（供多服務互通）：

docker network create my_network


安裝依賴：

uv sync

🌍 環境變數（.env）

程式會讀取 .env，缺省時使用預設值。此檔不要入版控。建議提供一份 env.example 供他人複製。

# Docker Hub (Airflow 自行 build 的 image tag)
DOCKER_HUB_USER=<your_dockerhub_username>  # 請改成你的 Docker Hub 帳號

# MySQL（給程式/ETL 使用）
MYSQL_HOST=127.0.0.1   # 本機跑腳本用；Airflow 容器內會覆寫為 mysql
MYSQL_PORT=3306
MYSQL_DB=ETF
MYSQL_USER=app
MYSQL_PASSWORD=

# Metabase（應用設定 DB）
MB_DB_USER=metabase
MB_DB_PASS=

# Airflow 管理員
AIRFLOW_ADMIN_USER=airflow
AIRFLOW_ADMIN_PASS=

# BigQuery / GCP
GCP_PROJECT_ID=etfproject20250923
BQ_DATASET_RAW=etf_raw
BQ_DATASET_ANALYTICS=etf_analytics
GOOGLE_APPLICATION_CREDENTIALS=/home/chris/ETF_DataAnalysis/key.json  # 本機路徑；Airflow 內會掛載到 /opt/airflow/key.json

# 指標與修正參數
SPLIT_THRESHOLD=0.20  # 拆/合股偵測（非除息日且跳動幅度）


在 本機 執行 Python 腳本時，MYSQL_HOST=127.0.0.1；在 Airflow 容器執行時，DAG 會將 MYSQL_HOST 覆寫為 mysql（與 Compose 服務名一致）。

🗄️ 啟動服務
MySQL + phpMyAdmin
docker compose -f docker-compose-mysql.yml up -d


phpMyAdmin：http://localhost:8000

Compose 內已設 --max_allowed_packet=128M，避免建立大型 VIEW 時斷線。

Metabase
docker compose -f docker-compose-metabase.yml up -d


Web：http://localhost:3000

首次登入後新增 MySQL 連線（依照 .env 配置）。

Airflow（DAG 排程）

build 專用 Airflow image（內含專案依賴與 dags）：

docker compose -f airflow/docker-compose-airflow.yml build --no-cache


啟動 Airflow：

docker compose -f airflow/docker-compose-airflow.yml up -d


Web：http://localhost:8080

首次初始化（若尚未 init）：

docker compose -f airflow/docker-compose-airflow.yml up -d airflow-init

🚀 本機執行流程（不走 Airflow）

建議先 source .env（或每條 uv run 後加 --env-file .env）。

載入環境變數

source .env


抓 ETF 配息資料

uv run -m data_ingestion.ETF_crawler_dividend


抓 ETF 歷史日價

uv run -m data_ingestion.ETF_crawler_price


計算指標並物化到 etf_metrics_daily

uv run -m data_ingestion.metrics_pipeline

☁️ BigQuery：輕量 ELT

專案採用 輕量 ELT：將 MySQL 原始表同步至 BQ_DATASET_RAW，再於 BQ_DATASET_ANALYTICS 建立視圖/物化表，供查詢與視覺化使用。

同步：MySQL → BigQuery Raw

etf_day_price（分區：trade_date；叢集：ticker）

etf_dividend（分區：ex_date；叢集：ticker）

etf_metrics_daily（分區：trade_date；叢集：ticker）

轉換：BigQuery Analytics

在 BigQuery 建立視圖與物化表，封裝分析邏輯（例如日/週/月聚合、指標篩選）。範例 SQL 見 data_ingestion/etf_bigquery_transform.py。

本機直跑
# 安裝 BQ 相關套件（若未安裝）
uv add google-cloud-bigquery pandas pyarrow pandas-gbq pymysql SQLAlchemy

# 同步 MySQL → BigQuery Raw
uv run --env-file .env -m data_ingestion.etf_sync_mysql_to_bigquery

# 建立 View / 物化表（Analytics）
uv run --env-file .env -m data_ingestion.etf_bigquery_transform

使用 Airflow（推薦）

DAG：airflow/dags/ETF_bigquery_etl_dag.py

流程：sync_mysql_to_bigquery → bigquery_transform

在容器內，DAG 自動設定：MYSQL_HOST=mysql、GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/key.json。

🗄️ 資料表說明
etf_dividend（唯一鍵：ticker + ex_date）

ticker, short_name, ex_date, record_date, payable_date

cash_dividend – 單位配息金額（彙總同日多筆）。

created_at, updated_at – 時間戳記。

etf_day_price（唯一鍵：ticker + trade_date）

volume, amount – 交易量與成交金額（整數）。

open, high, low, close – 開高低收價（小數）。

adjusted_close – 還原價（根據配息與拆合股調整）。

trades, created_at, updated_at – 成交筆數與時間戳記。

etf_metrics_daily（唯一鍵：ticker + trade_date）

價格 / 報酬

adjusted_close — 還原價，消除拆/合股與除息的跳動。

daily_return — 日報酬率 adj_t/adj_{t-1} - 1。

tri_total_return — 成立以來累積報酬 Π (1 + daily_return) - 1。

風險 / 回撤

vol_252 — 年化波動度 std(daily_return_{252}) * sqrt(252)。

sharpe_252d — 年化夏普值 (mean/std) * sqrt(252)（RF=0）。

drawdown — 回撤 adj_t / max(adj_{≤t}) - 1。

mdd — 最大回撤 min(drawdown_{≤t})。

股利 / 殖利率

dividend_12m — 近 12 個月現金股利總額。

dividend_yield_12m — 近 12 個月殖利率 dividend_12m / adjusted_close。

🆕 指標

以下指標用於區間回測與 KPI 概覽。公式採可讀寫法，簡潔易懂：

1) 總報酬率（Total Return, total_return）

用途：衡量整段投資期間的整體漲跌幅。

公式：

(期末資產 ÷ 期初資產) − 1

等價於複利連乘：∏(1 + r_t) − 1，其中 r_t 為日報酬率。

2) 年化報酬率（Compound Annual Growth Rate, cagr）

用途：把整段報酬換算成每年的穩定成長率，便於不同期間與產品比較。

公式：

CAGR = (期末資產 ÷ 期初資產)^(365/實際天數) − 1

3) 最大回撤（Max Drawdown, max_drawdown）

用途：評估「最壞情況會跌多深」。

公式：

(谷底 − 高峰) ÷ 高峰，其中谷底必須發生在高峰之後，整段期間取最小值。

4) 年化波動率（Annualized Volatility, vol_ann）

用途：衡量價格/報酬的波動程度，是 Sharpe 比率的分母。

公式：

σ_年 = σ_日 × √252，σ_日 為期間內日報酬率的標準差。

5) 夏普值（Sharpe Ratio, sharpe_ratio）

用途：每承擔 1 單位波動風險可得到多少報酬；數值越高越好。

公式：

Sharpe = 年化報酬 ÷ 年化波動（本專案假設無風險利率 RF = 0）。

6) 近 12 個月殖利率平均 (div_yield_12m_avg)

用途：觀察期間內的股息收益水準。

公式：

平均( 近 12 個月現金股利 ÷ 當日價格 )，對期間取平均。

7) 近 12 個月現金股利（最新） (dividend_12m_latest)

用途：呈現期間結束當天的 12 個月累計現金股利（TTM）。

公式：

取期末日的 dividend_12m（若期末日為空，取最近一筆非空值）。

📊 Metabase 面板卡片

使用 Metabase 的 Field Filter 功能建立交互式儀表板，對市值型與高股息型 ETF 進行視覺化分析。

全域篩選器

ticker（多選：支援一次選擇多檔 ETF）。

date range（以 trade_date 為日期範圍）。

卡片 A：月平均累積報酬率（折線圖）

此卡片展示所選 ETF 在自訂日期區間內「月平均累積報酬率」的走勢，方便比較不同檔 ETF 的長期趨勢。

目的：以月為尺度，橫向比較多檔 ETF 的累積報酬表現。

維度：trade_date（按月分組）。

度量：total_return 聚合後的月度累積值。

分組：ticker 作為多序列；Y 軸顯示為百分比。

卡片 B：ETF 概覽（表格）

此卡片彙總所選區間內各檔 ETF 的績效與風險指標，支援排序與篩選，可快速比較市值型與高股息型表現。

目的：提供區間 KPI 一覽，協助判斷各檔 ETF 的報酬與風險特性。

欄位（依儀表板實際順序）：

ETF代碼, 期間起, 期間迄, max_drawdown（最大回撤 MDD）, vol_ann（年化波動率）, sharpe_ratio（夏普值）, div_yield_12m_avg（殖利率〈近 12 個月〉）, total_return（總報酬率）, cagr（年化報酬率）。

互動：可依任一欄位（如 總報酬率 或 最大回撤）進行排序；透過 ticker 與 date range 篩選動態刷新 KPI。兩類 ETF 的表現可在此一覽無遺。

🛠️ 疑難排解
MySQL 斷線 / 大結果集

Compose 已設 --max_allowed_packet=128M；必要時重建：

docker compose -f docker-compose-mysql.yml up -d --force-recreate

Airflow DAG 失敗

檢查 Scheduler 容器內環境：

docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '\
  grep -E "GCP_PROJECT_ID|GOOGLE_APPLICATION_CREDENTIALS|MYSQL_HOST" /opt/airflow/.env || true\
  getent hosts mysql || true\
  ls -l /opt/airflow/key.json || true\
  python3 -c "import google.cloud.bigquery, pyarrow, pandas, pymysql, sqlalchemy; print(\'deps OK\')"\
'


手動在容器內跑腳本以排查錯誤：

docker compose -f airflow/docker-compose-airflow.yml exec airflow-scheduler bash -lc '\
  set -a; source /opt/airflow/.env; set +a\
  export PYTHONPATH=/opt/airflow\
  python3 -m data_ingestion.etf_sync_mysql_to_bigquery\
  python3 -m data_ingestion.etf_bigquery_transform\
'

BigQuery 權限錯誤

確認 .env 的金鑰路徑與容器內 /opt/airflow/key.json 一致，且服務帳戶擁有 BigQuery Admin、Job User、Storage Viewer 等權限。

📦 版本控與提交

提醒：將 key.json、.env 加入 .gitignore，避免憑證洩露。

git add .
git commit -m "docs: 更新 README、加入市值型與高股息型 ETF 清單並修正 TWSE 命名"
git push

📚 附錄：分析 ETF 清單（市值型 vs 高股息型）

以下列出本專案用於比較分析的 10 檔台股 ETF，各自屬於市值型或高股息型，您可直接複製此表至 README：

| 類型     | ETF 代碼 | ETF 名稱               |
|---------|---------|------------------------|
| 市值型   | 0050    | 元大台灣50             |
| 市值型   | 00881   | 國泰台灣科技龍頭       |
| 市值型   | 00850   | 元大台灣ESG永續        |
| 市值型   | 00922   | 國泰台灣領袖50         |
| 市值型   | 00692   | 富邦公司治理           |
| 高股息型 | 0056    | 元大高股息             |
| 高股息型 | 00713   | 元大台灣高息低波       |
| 高股息型 | 00878   | 國泰永續高股息         |
| 高股息型 | 00919   | 群益台灣精選高息       |
| 高股息型 | 00929   | 復華台灣科技高息30     |
