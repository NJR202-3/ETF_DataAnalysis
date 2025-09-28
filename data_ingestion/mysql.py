# -*- coding: utf-8 -*-
"""
mysql.py
- 集中管理 MySQL schema 與連線
- 提供 init_db() 與 upsert/批次上傳工具
"""

from __future__ import annotations

import os
from typing import List, Dict, Any

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, BigInteger,
    DECIMAL, Date, TIMESTAMP, text, UniqueConstraint
)
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert

# ------------------------------------------------------------
# 連線設定（環境變數優先；有預設值）
# ------------------------------------------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB",   "ETF")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "app123")

def _mysql_url() -> str:
    # 需已安裝 pymysql：pip install pymysql
    return f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"

# 全域 metadata（集中管理所有 Table）
metadata = MetaData()

# ------------------------------------------------------------
# 表：ETF 配息（唯一鍵：ticker + ex_date）
# ------------------------------------------------------------
etf_dividend = Table(
    "etf_dividend",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(16), nullable=False, comment="證券代號"),
    Column("short_name", String(64), nullable=True, comment="證券簡稱"),
    Column("ex_date", Date, nullable=False, comment="除息交易日"),
    Column("record_date", Date, nullable=True, comment="基準日"),
    Column("payable_date", Date, nullable=True, comment="發放日"),
    Column("cash_dividend", DECIMAL(12, 4), nullable=True, comment="現金股利/受益權益單位"),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False),
    UniqueConstraint("ticker", "ex_date", name="uq_etf_dividend_ticker_exdate"),
)

# ------------------------------------------------------------
# 表：ETF 日價（唯一鍵：ticker + trade_date）
#   *包含 adjusted_close（由 pipeline 計算 UPDATE），crawler 不會寫入*
# ------------------------------------------------------------
etf_day_price = Table(
    "etf_day_price",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(16), nullable=False, comment="證券代號"),
    Column("trade_date", Date, nullable=False, comment="交易日"),
    Column("volume", BigInteger, nullable=True, comment="成交股數"),
    Column("amount", BigInteger, nullable=True, comment="成交金額"),
    Column("open", DECIMAL(12, 3), nullable=True, comment="開盤價"),
    Column("high", DECIMAL(12, 3), nullable=True, comment="最高價"),
    Column("low", DECIMAL(12, 3), nullable=True, comment="最低價"),
    Column("close", DECIMAL(12, 3), nullable=True, comment="收盤價"),
    Column("adjusted_close", DECIMAL(16, 6), nullable=True, comment="還原收盤"),
    Column("trades", Integer, nullable=True, comment="成交筆數"),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False),
    UniqueConstraint("ticker", "trade_date", name="uq_etf_price_ticker_tradedate"),
)

# ------------------------------------------------------------
# 表：指標落地（唯一鍵：ticker + trade_date）
# ------------------------------------------------------------
etf_metrics_daily = Table(
    "etf_metrics_daily",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ticker", String(16), nullable=False),
    Column("trade_date", Date, nullable=False),
    Column("adjusted_close", DECIMAL(16, 6)),
    Column("daily_return", DECIMAL(16, 8)),
    Column("tri_total_return", DECIMAL(16, 8)),
    Column("vol_252", DECIMAL(16, 8)),
    Column("sharpe_60d", DECIMAL(16, 8)),
    Column("drawdown", DECIMAL(16, 8)),
    Column("mdd", DECIMAL(16, 8)),
    Column("dividend_12m", DECIMAL(16, 6)),
    Column("dividend_yield_12m", DECIMAL(16, 8)),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False),
    UniqueConstraint("ticker", "trade_date", name="uq_metrics"),
)

# ------------------------------------------------------------
# 建立資料庫表（若不存在才建立）＋ 輕量遷移保險
# ------------------------------------------------------------
def init_db() -> None:
    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    metadata.create_all(engine, tables=[etf_dividend, etf_day_price, etf_metrics_daily])

    # 輕量遷移：舊庫補欄位 / 補索引（MySQL 8 沒有 CREATE INDEX IF NOT EXISTS → 先查再建）
    with engine.begin() as conn:
        # 確保 adjusted_close 存在（舊環境沒有時補上）
        cnt = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=:db AND TABLE_NAME='etf_day_price' AND COLUMN_NAME='adjusted_close'
        """), {"db": MYSQL_DB}).scalar()
        if (cnt or 0) == 0:
            conn.execute(text("ALTER TABLE etf_day_price ADD COLUMN adjusted_close DECIMAL(16,6) NULL"))

        # UNIQUE 索引保險
        def _ensure_unique_idx(tbl: str, idx: str):
            q = text("""
                SELECT COUNT(*) FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA=:db AND TABLE_NAME=:tbl AND INDEX_NAME=:idx
            """)
            c = conn.execute(q, {"db": MYSQL_DB, "tbl": tbl, "idx": idx}).scalar()
            if (c or 0) == 0:
                if tbl == "etf_day_price" and idx == "uq_etf_price_ticker_tradedate":
                    conn.execute(text("CREATE UNIQUE INDEX uq_etf_price_ticker_tradedate ON etf_day_price (ticker, trade_date)"))
                elif tbl == "etf_dividend" and idx == "uq_etf_dividend_ticker_exdate":
                    conn.execute(text("CREATE UNIQUE INDEX uq_etf_dividend_ticker_exdate ON etf_dividend (ticker, ex_date)"))
                elif tbl == "etf_metrics_daily" and idx == "uq_metrics":
                    conn.execute(text("CREATE UNIQUE INDEX uq_metrics ON etf_metrics_daily (ticker, trade_date)"))

        _ensure_unique_idx("etf_day_price", "uq_etf_price_ticker_tradedate")
        _ensure_unique_idx("etf_dividend", "uq_etf_dividend_ticker_exdate")
        _ensure_unique_idx("etf_metrics_daily", "uq_metrics")

# ------------------------------------------------------------
# UPSERT：依 UNIQUE/PK 做更新（批次執行）
# 只更新「本次 INSERT 有帶到」且「非主鍵／非時間戳」的欄位
# ------------------------------------------------------------
def upload_data_to_mysql_upsert(table_obj: Table, data: List[Dict[str, Any]]) -> None:
    if not data:
        return

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    metadata.create_all(engine, tables=[table_obj])

    # 資料表欄位 / 主鍵
    column_names = [c.name for c in table_obj.columns]
    pk_names = {c.name for c in table_obj.primary_key.columns}

    # 這批資料「實際要 INSERT 的欄位」（任一列有帶到就算）
    inserted_keys = set()
    for row in data:
        inserted_keys |= set(row.keys())

    # 只允許更新：同時「在表中存在」且「這次 INSERT 有帶到」且「不是 PK/時間戳」的欄位
    updatable_cols = [
        c for c in column_names
        if c in inserted_keys and c not in pk_names and c not in ("created_at", "updated_at")
    ]

    with engine.begin() as conn:
        ins = insert(table_obj)
        update_dict = {c: ins.inserted[c] for c in updatable_cols}
        stmt = ins.on_duplicate_key_update(**update_dict)
        conn.execute(stmt, data)   # 批次 UPSERT

# ------------------------------------------------------------
# 批次整表覆蓋（可選）：用 pandas.to_sql
# ------------------------------------------------------------
def upload_dataframe(table_name: str, df, mode: str = "replace") -> None:
    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists=mode, index=False)
