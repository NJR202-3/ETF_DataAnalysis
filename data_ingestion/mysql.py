# -*- coding: utf-8 -*-
"""
data_ingestion/mysql.py
- 管理 MySQL 連線與 Schema
- 提供 init_db() 與通用 UPSERT 上傳工具
- 不強制要求環境變數，若未設定則使用安全預設值（可用 .env 覆寫）
"""

from __future__ import annotations

import os
from typing import List, Dict, Any

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, Integer, BigInteger, DECIMAL, Date, TIMESTAMP, text, UniqueConstraint
)
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.mysql import insert

# ------------------------------------------------------------
# 連線設定（環境變數優先；沒設就用預設）
# ------------------------------------------------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DB",   "ETF")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "app123")

def _mysql_url() -> str:
    # 需已安裝 PyMySQL：pip install PyMySQL
    return f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"

def get_engine() -> Engine:
    return create_engine(_mysql_url(), pool_pre_ping=True, future=True)

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
    Column("low",  DECIMAL(12, 3), nullable=True, comment="最低價"),
    Column("close", DECIMAL(12, 3), nullable=True, comment="收盤價"),
    Column("adjusted_close", DECIMAL(16, 6), nullable=True, comment="還原收盤"),
    Column("trades", Integer, nullable=True, comment="成交筆數"),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False),
    UniqueConstraint("ticker", "trade_date", name="uq_etf_price_ticker_tradedate"),
)

# ------------------------------------------------------------
# 表：指標落地（唯一鍵：ticker + trade_date）
#   注意：欄位已更新為 vol_252 與 sharpe_252d（符合你最新需求）
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
    Column("sharpe_252d", DECIMAL(16, 8)),
    Column("drawdown", DECIMAL(16, 8)),
    Column("mdd", DECIMAL(16, 8)),
    Column("dividend_12m", DECIMAL(16, 6)),
    Column("dividend_yield_12m", DECIMAL(16, 8)),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False),
    UniqueConstraint("ticker", "trade_date", name="uq_metrics"),
)

# ------------------------------------------------------------
# 建立資料表（若不存在才建立）
# ------------------------------------------------------------
def init_db() -> None:
    engine = get_engine()
    metadata.create_all(engine, tables=[etf_dividend, etf_day_price, etf_metrics_daily])

# ------------------------------------------------------------
# UPSERT：依 UNIQUE/PK 做更新（批次執行）
# 只更新「這批有帶到」且「非主鍵／非時間戳」的欄位
# ------------------------------------------------------------
def upload_data_to_mysql_upsert(table_obj: Table, data: List[Dict[str, Any]]) -> None:
    if not data:
        return

    engine = get_engine()
    metadata.create_all(engine, tables=[table_obj])

    # 這批資料實際出現的欄位集合
    inserted_keys = set()
    for row in data:
        inserted_keys |= set(row.keys())

    # 決定哪些欄位在 ON DUPLICATE KEY UPDATE 時可以更新
    pk_names = {c.name for c in table_obj.primary_key.columns}
    updatable_cols = [
        c.name for c in table_obj.columns
        if c.name in inserted_keys and c.name not in pk_names and c.name not in ("created_at", "updated_at")
    ]

    with engine.begin() as conn:
        ins = insert(table_obj)
        update_dict = {c: ins.inserted[c] for c in updatable_cols}
        stmt = ins.on_duplicate_key_update(**update_dict)
        conn.execute(stmt, data)  # 批次 UPSERT
