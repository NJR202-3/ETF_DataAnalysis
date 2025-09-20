# -*- coding: utf-8 -*-
"""
metrics_pipeline.py
- 從 MySQL 讀取 etf_day_price / etf_dividend
- 以「跳動 >= 20% 且非除息日」偵測拆/合股，更新 etf_day_price.adjusted_close
- 以 SQL 建 vw_etf_metrics（指標檢視）
- 另將檢視資料寫入實體表 etf_metrics_daily（主鍵：ticker, trade_date）
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Dict, List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

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


# ======== 工具：欄位是否存在 ========
def column_exists(engine: Engine, table: str, column: str) -> bool:
    sql = text(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl AND COLUMN_NAME = :col
        """
    )
    with engine.begin() as conn:
        cnt = conn.execute(sql, {"db": MYSQL_DB, "tbl": table, "col": column}).scalar()
    return (cnt or 0) > 0


# ======== 第 1 部分：建立 / 填補 adjusted_close ========
def build_adjusted_close(engine: Engine, threshold: float = SPLIT_THRESHOLD) -> None:
    """
    規則：
    - 以每檔 ticker 依日期排序，計算 ratio = today_close / prev_close
    - 若 |ratio - 1| >= threshold 且 今日非除息日 -> 視為拆/合股事件，將累積係數 *= (prev_close / today_close)
    - adjusted_close = close * 累積係數
    備註：
    - 為避免極端誤判，ratio 介於 [0.2, 5] 以內才視為有效事件
    """
    with engine.begin() as conn:
        # 1) 若沒有 adjusted_close 欄位，就新增
        if not column_exists(engine, "etf_day_price", "adjusted_close"):
            conn.execute(text("ALTER TABLE etf_day_price ADD COLUMN adjusted_close DECIMAL(16,6) NULL"))

        # 2) 先把既有為 NULL 的 adjusted_close 填上 close（避免空值）
        conn.execute(text("UPDATE etf_day_price SET adjusted_close = close WHERE adjusted_close IS NULL"))

        # 3) 取得所有 ticker
        tickers = conn.execute(text("SELECT DISTINCT ticker FROM etf_day_price")).scalars().all()

        for tk in tickers:
            # 取該檔所有交易日 close，並帶出是否除息
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

            # 累積係數（由過去往現在遞推）：預設 1
            factors: List[Decimal] = []
            cum = Decimal("1.0")

            prev_close = Decimal(str(rows[0].close))
            factors.append(cum)  # 第一筆沿用 1 * close

            for i in range(1, len(rows)):
                today_close = Decimal(str(rows[i].close))
                is_div = int(rows[i].is_div) == 1

                # ratio：今日 vs 前日
                # 避免除以 0
                if prev_close and today_close:
                    ratio = today_close / prev_close  # ~ 1/k（拆股）或 k（合股）
                else:
                    ratio = Decimal("1.0")

                jump = abs(ratio - Decimal("1.0"))

                # 僅在「非除息」且跳動 >= 門檻時視為拆/合股；另外過濾不合理極端值
                if (not is_div) and (jump >= Decimal(str(threshold))) and (Decimal("0.2") <= ratio <= Decimal("5.0")):
                    # 估計係數（回推到前日同基準）
                    # 若今日是 1:2 拆股，today_close ~ prev_close / 2，ratio ~ 0.5
                    # 我們取 factor = prev_close / today_close
                    step_factor = (prev_close / today_close)
                    # 更新累積係數（歷史資料要乘上更多係數）
                    cum = cum * step_factor

                factors.append(cum)
                prev_close = today_close

            # 計算 adjusted_close = close * factor，批次寫回
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


# ======== 第 2 部分：建立檢視 / 實體表 ========
def create_metrics_view(engine: Engine) -> None:
    """
    建立/重建 vw_etf_metrics
    指標：
      - daily_return: adj / LAG(adj) - 1
      - tri_total_return: 累乘(1+ret)-1 = EXP(SUM(LOG(1+ret))) - 1
      - vol_252: 最近 252 根日報酬的 STDDEV_POP
      - sharpe_60d: 最近 60 根平均/標準差 * sqrt(252)
      - drawdown: adj / running_max(adj) - 1
      - mdd: min(drawdown) over (ticker)
      - dividend_12m: 近 365 天股利總額
      - dividend_yield_12m: dividend_12m / adjusted_close
    """
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
      -- 日報酬
      (p.adjusted_close / NULLIF(LAG(p.adjusted_close) OVER (PARTITION BY p.ticker ORDER BY p.trade_date), 0)) - 1
        AS daily_return,
      -- 當日回撤（相對歷史新高）
      (p.adjusted_close / NULLIF(MAX(p.adjusted_close) OVER (PARTITION BY p.ticker ORDER BY p.trade_date 
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) - 1 AS drawdown
  FROM etf_day_price p
),
base2 AS (
  SELECT
    b.*,
    -- TRI（累積報酬）
    EXP(SUM(LOG(1 + COALESCE(b.daily_return,0))) OVER (
        PARTITION BY b.ticker ORDER BY b.trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) - 1 AS tri_total_return,
    -- 252 日年化波動（用日標準差 * sqrt(252)）
    STDDEV_POP(b.daily_return) OVER (
        PARTITION BY b.ticker ORDER BY b.trade_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    ) * SQRT(252) AS vol_252,
    -- 60 日 Sharpe（不扣無風險利率）
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
    -- 同檔整段 MDD
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
  -- 近 12 個月現金股利總額
  (
    SELECT SUM(d.cash_dividend)
    FROM etf_dividend d
    WHERE d.ticker = x.ticker
      AND d.ex_date BETWEEN DATE_SUB(x.trade_date, INTERVAL 365 DAY) AND x.trade_date
  ) AS dividend_12m,
  -- 殖利率（近 12 個月股利 / 當日還原收盤）
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


def upsert_materialized_table(engine: Engine) -> None:
    """
    建立實體表 etf_metrics_daily，並把檢視灌進來（以 ticker+trade_date 當唯一鍵 upsert）
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
CREATE TABLE IF NOT EXISTS etf_metrics_daily (
  id INT AUTO_INCREMENT PRIMARY KEY,
  ticker VARCHAR(16) NOT NULL,
  trade_date DATE NOT NULL,
  adjusted_close DECIMAL(16,6) NULL,
  daily_return DECIMAL(16,8) NULL,
  tri_total_return DECIMAL(16,8) NULL,
  vol_252 DECIMAL(16,8) NULL,
  sharpe_60d DECIMAL(16,8) NULL,
  drawdown DECIMAL(16,8) NULL,
  mdd DECIMAL(16,8) NULL,
  dividend_12m DECIMAL(16,6) NULL,
  dividend_yield_12m DECIMAL(16,8) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_metrics (ticker, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )

        # 將檢視資料 upsert 進表
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
