# -*- coding: utf-8 -*-
"""
metrics_pipeline.py
- 從 MySQL 讀取 etf_day_price / etf_dividend
- 以「跳動 >= 20% 且非除息日」偵測拆/合股，更新 etf_day_price.adjusted_close
- 以 SQL 建 vw_etf_metrics（指標檢視）
- 將檢視資料寫入實體表 etf_metrics_daily（唯一鍵：ticker, trade_date）
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# <<< 新增：集中由 mysql.init_db 建表 / 索引 >>>
from data_ingestion.mysql import init_db

# ======== 連線參數（環境變數優先） ========
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB",   "ETF")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "app123")

# 拆股偵測門檻（20%）
SPLIT_THRESHOLD = float(os.getenv("SPLIT_THRESHOLD", "0.20"))

def _url() -> str:
    return (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    )

def get_engine() -> Engine:
    return create_engine(_url(), pool_pre_ping=True, future=True)

# ======== 第 1 部分：建立 / 填補 adjusted_close ========
def build_adjusted_close(engine: Engine, threshold: float = SPLIT_THRESHOLD) -> None:
    """
    規則：
    - 以每檔 ticker 依日期排序，計算 ratio = today_close / prev_close
    - 若 |ratio - 1| >= threshold 且 今日非除息日 -> 視為拆/合股事件，將累積係數 *= (prev_close / today_close)
    - adjusted_close = close * 累積係數
    - 過濾極端值：ratio 介於 [0.2, 5] 才視為有效事件
    """
    with engine.begin() as conn:
        # 先把尚未有值的 adjusted_close 補上 close（避免空值）
        conn.execute(text("UPDATE etf_day_price SET adjusted_close = close WHERE adjusted_close IS NULL"))

        # 取得所有 ticker
        tickers = conn.execute(text("SELECT DISTINCT ticker FROM etf_day_price")).scalars().all()

        for tk in tickers:
            rows = conn.execute(
                text(
                    """
                    SELECT p.trade_date, p.close, 
                           CASE WHEN d.ex_date IS NULL THEN 0 ELSE 1 END AS is_div
                    FROM etf_day_price p
                    LEFT JOIN etf_dividend d
                      ON d.ticker = p.ticker AND d.ex_date = p.trade_date
                    WHERE p.ticker = :tk
                    ORDER BY p.trade_date
                    """
                ),
                {"tk": tk},
            ).all()
            if not rows:
                continue

            factors: List[Decimal] = []
            cum = Decimal("1.0")

            prev_close = Decimal(str(rows[0].close))
            factors.append(cum)

            for i in range(1, len(rows)):
                today_close = Decimal(str(rows[i].close))
                is_div = int(rows[i].is_div) == 1

                ratio = today_close / prev_close if prev_close and today_close else Decimal("1.0")
                jump = abs(ratio - Decimal("1.0"))

                if (not is_div) and (jump >= Decimal(str(threshold))) and (Decimal("0.2") <= ratio <= Decimal("5.0")):
                    step_factor = (prev_close / today_close)
                    cum = cum * step_factor

                factors.append(cum)
                prev_close = today_close

            payload: List[Tuple[str, str, str]] = []
            for (r, f) in zip(rows, factors):
                adj = (Decimal(str(r.close)) * f).quantize(Decimal("0.000001"))
                payload.append((tk, str(r.trade_date), str(adj)))

            conn.execute(
                text(
                    """
                    UPDATE etf_day_price 
                    SET adjusted_close = :adj
                    WHERE ticker = :tk AND trade_date = :dt
                    """
                ),
                [{"tk": tk, "dt": dt, "adj": adj} for (tk, dt, adj) in payload],
            )

        # 補強：仍為 NULL 的（理論上不會）再以 close 帶上
        conn.execute(text("UPDATE etf_day_price SET adjusted_close = close WHERE adjusted_close IS NULL"))

# ======== 第 2 部分：建立檢視 ========
def create_metrics_view(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS vw_etf_metrics"))
        conn.execute(
            text(
                """
CREATE VIEW vw_etf_metrics AS
WITH base AS (
  SELECT
      p.ticker,
      p.trade_date,
      p.adjusted_close,
      (p.adjusted_close / NULLIF(LAG(p.adjusted_close) OVER (PARTITION BY p.ticker ORDER BY p.trade_date), 0)) - 1
        AS daily_return,
      (p.adjusted_close / NULLIF(MAX(p.adjusted_close) OVER (PARTITION BY p.ticker ORDER BY p.trade_date 
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) - 1 AS drawdown
  FROM etf_day_price p
),
base2 AS (
  SELECT
    b.*,
    EXP(SUM(LOG(1 + COALESCE(b.daily_return,0))) OVER (
        PARTITION BY b.ticker ORDER BY b.trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) - 1 AS tri_total_return,
    STDDEV_POP(b.daily_return) OVER (
        PARTITION BY b.ticker ORDER BY b.trade_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) * SQRT(252) AS vol_252,
    AVG(b.daily_return) OVER (
        PARTITION BY b.ticker ORDER BY b.trade_date
        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
    ) / NULLIF(
        STDDEV_POP(b.daily_return) OVER (
            PARTITION BY b.ticker ORDER BY b.trade_date
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ),
        0
    ) * SQRT(252) AS sharpe_60d,
    MIN(b.drawdown) OVER (PARTITION BY b.ticker) AS mdd
  FROM base b
)
SELECT
  x.ticker,
  x.trade_date,
  x.adjusted_close,
  x.daily_return,
  x.tri_total_return,
  x.vol_252,
  x.sharpe_60d,
  x.drawdown,
  x.mdd,
  (
    SELECT SUM(d.cash_dividend)
    FROM etf_dividend d
    WHERE d.ticker = x.ticker
      AND d.ex_date BETWEEN DATE_SUB(x.trade_date, INTERVAL 365 DAY) AND x.trade_date
  ) AS dividend_12m,
  (
    SELECT SUM(d.cash_dividend)
    FROM etf_dividend d
    WHERE d.ticker = x.ticker
      AND d.ex_date BETWEEN DATE_SUB(x.trade_date, INTERVAL 365 DAY) AND x.trade_date
  ) / NULLIF(x.adjusted_close, 0) AS dividend_yield_12m
FROM base2 x
                """
            )
        )

# ======== 第 3 部分：把檢視資料 UPSERT 到實體表 ========
def upsert_materialized_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
INSERT INTO etf_metrics_daily
(ticker, trade_date, adjusted_close, daily_return, tri_total_return, vol_252, sharpe_60d, drawdown, mdd, dividend_12m, dividend_yield_12m)
SELECT
  ticker, trade_date, adjusted_close, daily_return, tri_total_return, vol_252, sharpe_60d, drawdown, mdd, dividend_12m, dividend_yield_12m
FROM vw_etf_metrics
ON DUPLICATE KEY UPDATE
  adjusted_close      = VALUES(adjusted_close),
  daily_return        = VALUES(daily_return),
  tri_total_return    = VALUES(tri_total_return),
  vol_252             = VALUES(vol_252),
  sharpe_60d          = VALUES(sharpe_60d),
  drawdown            = VALUES(drawdown),
  mdd                 = VALUES(mdd),
  dividend_12m        = VALUES(dividend_12m),
  dividend_yield_12m  = VALUES(dividend_yield_12m),
  updated_at          = CURRENT_TIMESTAMP
                """
            )
        )

# ======== 入口 ========
if __name__ == "__main__":
    # 一行確保 schema 齊全（集中在 mysql.py）
    init_db()

    eng = get_engine()

    print("Step 1/3 生成 adjusted_close（偵測拆/合股）...")
    build_adjusted_close(eng, threshold=SPLIT_THRESHOLD)
    print("  ✓ 完成")

    print("Step 2/3 建立/更新檢視 vw_etf_metrics ...")
    create_metrics_view(eng)
    print("  ✓ 完成")

    print("Step 3/3 同步到實體表 etf_metrics_daily ...")
    upsert_materialized_table(eng)
    print("  ✓ 完成\n全部 OK！")
