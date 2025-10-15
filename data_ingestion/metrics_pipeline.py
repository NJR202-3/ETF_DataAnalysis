# -*- coding: utf-8 -*-
# 計算含息總報酬（TRI）與純價格報酬；Sharpe/Vol 用 252 日視窗；物化到 etf_metrics_daily

import os
from typing import List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from data_ingestion.mysql import init_db

# ── 連線參數 ─────────────────────────────────────────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB",   "ETF")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "app123")

# 只把「非除息且跳動幅度過大」視為拆/合股
SPLIT_THRESHOLD = float(os.getenv("SPLIT_THRESHOLD", "0.20"))

def _url() -> str:
    return f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"

def get_engine() -> Engine:
    return create_engine(_url(), pool_pre_ping=True, future=True)

# ── 1) 生成 adjusted_close（僅拆/合股校正，不動除息） ──────────────────────────
def build_adjusted_close(engine: Engine, threshold: float = SPLIT_THRESHOLD) -> None:
    with engine.begin() as conn:
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

            fac = 1.0
            prev = float(rows[0].close)
            payload: List[Tuple[str, str, float]] = []
            payload.append((tk, str(rows[0].trade_date), round(prev * fac, 6)))

            for i in range(1, len(rows)):
                close = float(rows[i].close)
                is_div = int(rows[i].is_div) == 1
                ratio = close / prev if prev else 1.0
                # 非除息且跳動過大 → 視為拆/合股校正因子
                if (not is_div) and abs(ratio - 1.0) >= threshold and 0.2 <= ratio <= 5.0:
                    fac *= (prev / close)
                adj = round(close * fac, 6)
                payload.append((tk, str(rows[i].trade_date), adj))
                prev = close

            conn.execute(
                text("UPDATE etf_day_price SET adjusted_close = :adj WHERE ticker = :tk AND trade_date = :dt"),
                [{"tk": t, "dt": d, "adj": a} for (t, d, a) in payload],
            )

# ── 2) 建檢視：日報酬(價格/含息)、TRI、Sharpe/Vol、MDD、TTM 股利/殖利率 ────────────────
def create_metrics_view(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS vw_etf_metrics"))
        conn.execute(text("""
CREATE VIEW vw_etf_metrics AS
WITH
d_agg AS (  -- 同日多筆股利先彙總
  SELECT ticker, ex_date, SUM(cash_dividend) AS cash_div
  FROM etf_dividend
  GROUP BY ticker, ex_date
),
base AS (
  SELECT
    p.ticker,
    p.trade_date,
    p.adjusted_close,
    COALESCE(d.cash_div, 0) AS cash_div_today
  FROM etf_day_price p
  LEFT JOIN d_agg d
    ON d.ticker = p.ticker AND d.ex_date = p.trade_date
),
ret AS (
  SELECT
    b.*,
    -- 純價格日報酬（第一天定義為 0）
    CASE
      WHEN LAG(b.adjusted_close) OVER (PARTITION BY b.ticker ORDER BY b.trade_date) IS NULL
        THEN 0
      ELSE (b.adjusted_close / NULLIF(LAG(b.adjusted_close) OVER (PARTITION BY b.ticker ORDER BY b.trade_date),0)) - 1
    END AS daily_return_px,
    -- 含股息日報酬（除息日把 D_t 加到分子）
    CASE
      WHEN LAG(b.adjusted_close) OVER (PARTITION BY b.ticker ORDER BY b.trade_date) IS NULL
        THEN 0
      ELSE ((b.adjusted_close + b.cash_div_today) / NULLIF(LAG(b.adjusted_close) OVER (PARTITION BY b.ticker ORDER BY b.trade_date),0)) - 1
    END AS daily_return_tri,
    -- 逐日峰值用於 drawdown
    (b.adjusted_close / NULLIF(MAX(b.adjusted_close) OVER (
      PARTITION BY b.ticker ORDER BY b.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),0)) - 1 AS drawdown
  FROM base b
),
win AS (
  SELECT
    r.*,
    COUNT(*) OVER (PARTITION BY r.ticker ORDER BY r.trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS cnt252,
    AVG(r.daily_return_px) OVER (PARTITION BY r.ticker ORDER BY r.trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS mu252,
    STDDEV_POP(r.daily_return_px) OVER (PARTITION BY r.ticker ORDER BY r.trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS sd252,
    -- 累積總報酬（純價格）
    EXP(SUM(LOG(1 + r.daily_return_px)) OVER (
      PARTITION BY r.ticker ORDER BY r.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) - 1 AS total_return_px,
    -- 累積總報酬（含股息再投資）
    EXP(SUM(LOG(1 + r.daily_return_tri)) OVER (
      PARTITION BY r.ticker ORDER BY r.trade_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) - 1 AS tri_total_return_raw
  FROM ret r
)
SELECT
  w.ticker,
  w.trade_date,
  w.adjusted_close,
  w.daily_return_px,
  w.daily_return_tri,
  w.total_return_px,
  w.tri_total_return_raw      AS tri_total_return,
  CASE WHEN w.cnt252 = 252 THEN w.sd252 * SQRT(252) ELSE NULL END AS vol_252,
  CASE WHEN w.cnt252 = 252 AND w.sd252 IS NOT NULL AND w.sd252 <> 0
       THEN (w.mu252 / w.sd252) * SQRT(252)
       ELSE NULL END AS sharpe_252d,
  w.drawdown,
  MIN(w.drawdown) OVER (PARTITION BY w.ticker) AS mdd,
  -- 近 12 個月現金股利與殖利率（用期末價格）
  (SELECT SUM(d.cash_dividend)
     FROM etf_dividend d
    WHERE d.ticker = w.ticker
      AND d.ex_date BETWEEN DATE_SUB(w.trade_date, INTERVAL 365 DAY) AND w.trade_date) AS dividend_12m,
  (SELECT SUM(d.cash_dividend)
     FROM etf_dividend d
    WHERE d.ticker = w.ticker
      AND d.ex_date BETWEEN DATE_SUB(w.trade_date, INTERVAL 365 DAY) AND w.trade_date) / NULLIF(w.adjusted_close,0) AS dividend_yield_12m
FROM win w;
        """))

# ── 3) 物化到 etf_metrics_daily ─────────────────────────────────────────────
def upsert_materialized_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
INSERT INTO etf_metrics_daily
(ticker, trade_date, adjusted_close,
 daily_return,             -- 兼容舊欄位：放「純價格」日報酬
 daily_return_tri,         -- 新增：含息日報酬
 tri_total_return,         -- 含息累積總報酬
 total_return,             -- 兼容舊欄位：放「純價格」累積總報酬
 vol_252, sharpe_252d, drawdown, mdd, dividend_12m, dividend_yield_12m)
SELECT
  ticker,
  trade_date,
  adjusted_close,
  daily_return_px                     AS daily_return,
  daily_return_tri,
  tri_total_return,
  total_return_px                     AS total_return,
  vol_252, sharpe_252d, drawdown, mdd, dividend_12m, dividend_yield_12m
FROM vw_etf_metrics
ON DUPLICATE KEY UPDATE
  adjusted_close     = VALUES(adjusted_close),
  daily_return       = VALUES(daily_return),
  daily_return_tri   = VALUES(daily_return_tri),
  tri_total_return   = VALUES(tri_total_return),
  total_return       = VALUES(total_return),
  vol_252            = VALUES(vol_252),
  sharpe_252d        = VALUES(sharpe_252d),
  drawdown           = VALUES(drawdown),
  mdd                = VALUES(mdd),
  dividend_12m       = VALUES(dividend_12m),
  dividend_yield_12m = VALUES(dividend_yield_12m),
  updated_at         = CURRENT_TIMESTAMP;
        """))

if __name__ == "__main__":
    init_db()
    eng = get_engine()
    print("Step 1/3 調整 adjusted_close（拆/合股校正） ...")
    build_adjusted_close(eng, threshold=SPLIT_THRESHOLD)
    print("Step 2/3 建/更新檢視（含息/純價報酬、TRI、Sharpe/Vol 等） ...")
    create_metrics_view(eng)
    print("Step 3/3 物化 etf_metrics_daily ...")
    upsert_materialized_table(eng)
    print("全部完成！")
