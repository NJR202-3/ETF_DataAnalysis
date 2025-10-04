# -*- coding: utf-8 -*-
# 產生 adjusted_close、建立檢視、物化到 etf_metrics_daily（Sharpe 改為 252 日視窗）

import os
from typing import List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from data_ingestion.mysql import init_db

# 連線參數（可用環境變數覆寫）
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB",   "ETF")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "app123")

# 拆/合股偵測門檻（>20% 且非除息日）
SPLIT_THRESHOLD = float(os.getenv("SPLIT_THRESHOLD", "0.20"))

def _url() -> str:
    return f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"

def get_engine() -> Engine:
    return create_engine(_url(), pool_pre_ping=True, future=True)

# 1) 生成 adjusted_close（把「非除息的大跳動」視為拆/合股，累乘校正係數）
def build_adjusted_close(engine: Engine, threshold: float = SPLIT_THRESHOLD) -> None:
    with engine.begin() as conn:
        # 先以 close 補齊空值
        conn.execute(text("UPDATE etf_day_price SET adjusted_close = close WHERE adjusted_close IS NULL"))
        tickers = conn.execute(text("SELECT DISTINCT ticker FROM etf_day_price")).scalars().all()

        for tk in tickers:
            rows = conn.execute(text("""
                SELECT p.trade_date, p.close,
                       CASE WHEN d.ex_date IS NULL THEN 0 ELSE 1 END AS is_div
                FROM etf_day_price p
                LEFT JOIN etf_dividend d
                  ON d.ticker = p.ticker AND d.ex_date = p.trade_date
                WHERE p.ticker = :tk
                ORDER BY p.trade_date
            """), {"tk": tk}).all()
            if not rows:
                continue

            # 簡單好懂：一路走訪，遇到「非除息且跳動>門檻」就更新累乘係數
            fac = 1.0
            prev = float(rows[0].close)
            payload: List[Tuple[str, str, float]] = []
            payload.append((tk, str(rows[0].trade_date), round(prev * fac, 6)))

            for i in range(1, len(rows)):
                close = float(rows[i].close)
                is_div = int(rows[i].is_div) == 1
                ratio = close / prev if prev else 1.0
                if (not is_div) and abs(ratio - 1.0) >= threshold and 0.2 <= ratio <= 5.0:
                    fac *= (prev / close)  # 拆/合股校正
                adj = round(close * fac, 6)
                payload.append((tk, str(rows[i].trade_date), adj))
                prev = close

            conn.execute(
                text("UPDATE etf_day_price SET adjusted_close = :adj WHERE ticker = :tk AND trade_date = :dt"),
                [{"tk": t, "dt": d, "adj": a} for (t, d, a) in payload],
            )

# 2) 建檢視（Sharpe/Vol 用 252 交易日視窗；daily_return 首日=0；股利先彙總）
def create_metrics_view(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS vw_etf_metrics"))
        conn.execute(text("""
CREATE VIEW vw_etf_metrics AS
WITH
d_agg AS (
  SELECT ticker, ex_date, SUM(cash_dividend) AS cash_div
  FROM etf_dividend
  GROUP BY ticker, ex_date
),
base AS (
  SELECT
    p.ticker,
    p.trade_date,
    p.adjusted_close,
    CASE
      WHEN LAG(p.adjusted_close) OVER (PARTITION BY p.ticker ORDER BY p.trade_date) IS NULL
        THEN 0
      ELSE (p.adjusted_close / NULLIF(LAG(p.adjusted_close) OVER (PARTITION BY p.ticker ORDER BY p.trade_date), 0)) - 1
    END AS daily_return,
    (p.adjusted_close / NULLIF(MAX(p.adjusted_close) OVER (
        PARTITION BY p.ticker ORDER BY p.trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 0)) - 1 AS drawdown
  FROM etf_day_price p
),
win AS (
  SELECT
    b.*,
    COUNT(*) OVER (
      PARTITION BY b.ticker ORDER BY b.trade_date
      ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS cnt252,
    -- 252 視窗的均值與標準差
    AVG(b.daily_return) OVER (
      PARTITION BY b.ticker ORDER BY b.trade_date
      ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS mu252,
    STDDEV_POP(b.daily_return) OVER (
      PARTITION BY b.ticker ORDER BY b.trade_date
      ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) AS sd252,
    -- 成立以來累積報酬（TRI-1）
    EXP(SUM(LOG(1 + b.daily_return)) OVER (
      PARTITION BY b.ticker ORDER BY b.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) - 1 AS tri_total_return_raw
  FROM base b
)
SELECT
  x.ticker,
  x.trade_date,
  x.adjusted_close,
  x.daily_return,
  x.tri_total_return_raw          AS tri_total_return,
  CASE WHEN x.cnt252 = 252 THEN x.sd252 * SQRT(252) ELSE NULL END AS vol_252,
  CASE WHEN x.cnt252 = 252 AND x.sd252 IS NOT NULL AND x.sd252 <> 0
       THEN (x.mu252 / x.sd252) * SQRT(252)
       ELSE NULL
  END AS sharpe_252d,
  x.drawdown,
  MIN(x.drawdown) OVER (PARTITION BY x.ticker) AS mdd,
  (
    SELECT SUM(d.cash_div)
    FROM d_agg d
    WHERE d.ticker = x.ticker
      AND d.ex_date BETWEEN DATE_SUB(x.trade_date, INTERVAL 365 DAY) AND x.trade_date
  ) AS dividend_12m,
  (
    SELECT SUM(d.cash_div)
    FROM d_agg d
    WHERE d.ticker = x.ticker
      AND d.ex_date BETWEEN DATE_SUB(x.trade_date, INTERVAL 365 DAY) AND x.trade_date
  ) / NULLIF(x.adjusted_close, 0) AS dividend_yield_12m
FROM win x;
        """))

# 3) 視圖資料 → 物化到 etf_metrics_daily（欄位名含 sharpe_252d）
def upsert_materialized_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
INSERT INTO etf_metrics_daily
(ticker, trade_date, adjusted_close, daily_return, tri_total_return, vol_252, sharpe_252d, drawdown, mdd, dividend_12m, dividend_yield_12m)
SELECT
  ticker, trade_date, adjusted_close, daily_return, tri_total_return, vol_252, sharpe_252d, drawdown, mdd, dividend_12m, dividend_yield_12m
FROM vw_etf_metrics
ON DUPLICATE KEY UPDATE
  adjusted_close      = VALUES(adjusted_close),
  daily_return        = VALUES(daily_return),
  tri_total_return    = VALUES(tri_total_return),
  vol_252             = VALUES(vol_252),
  sharpe_252d         = VALUES(sharpe_252d),
  drawdown            = VALUES(drawdown),
  mdd                 = VALUES(mdd),
  dividend_12m        = VALUES(dividend_12m),
  dividend_yield_12m  = VALUES(dividend_yield_12m),
  updated_at          = CURRENT_TIMESTAMP;
        """))

if __name__ == "__main__":
    init_db()
    eng = get_engine()
    print("Step 1/3 調整 adjusted_close ...")
    build_adjusted_close(eng, threshold=SPLIT_THRESHOLD)
    print("Step 2/3 建立/更新視圖 ...")
    create_metrics_view(eng)
    print("Step 3/3 物化 etf_metrics_daily ...")
    upsert_materialized_table(eng)
    print("全部完成！")
