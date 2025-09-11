# -*- coding: utf-8 -*-
"""
mysql.py
- 建立 MySQL 連線、資料表結構
- 提供 init_db() 與 upsert/批次上傳工具

用法：
from data_ingestion.mysql import init_db, upload_data_to_mysql_upsert, etf_dividend, etf_day_price
init_db()
upload_data_to_mysql_upsert(etf_dividend, rows)  # rows: List[dict]
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
    # 時間戳（可有可無；不在 UPSERT 更新清單中）
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False),
    UniqueConstraint("ticker", "ex_date", name="uq_etf_dividend_ticker_exdate"),
)

# ------------------------------------------------------------
# 表：ETF 日價（唯一鍵：ticker + trade_date）
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
    Column("trades", Integer, nullable=True, comment="成交筆數"),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), nullable=False),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"), nullable=False),
    UniqueConstraint("ticker", "trade_date", name="uq_etf_price_ticker_tradedate"),
)

# ------------------------------------------------------------
# 建立資料庫表（若不存在才建立）
# ------------------------------------------------------------
def init_db() -> None:
    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    metadata.create_all(engine, tables=[etf_dividend, etf_day_price])

# ------------------------------------------------------------
# UPSERT：依 Table 的 UniqueConstraint / Primary Key 做更新
#   - 只更新表內存在、且非主鍵、且不是 created_at/updated_at 的欄位
#   - data: List[dict]，鍵名需對應欄位名
# ------------------------------------------------------------
def upload_data_to_mysql_upsert(table_obj: Table, data: List[Dict[str, Any]]) -> None:
    if not data:
        return

    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    # 確保表存在（不會覆蓋）
    metadata.create_all(engine, tables=[table_obj])

    # 計算可更新欄位
    column_names = [c.name for c in table_obj.columns]
    pk_names = [c.name for c in table_obj.primary_key.columns]
    updatable_cols = [
        c for c in column_names
        if c not in pk_names and c not in ("created_at", "updated_at")
    ]

    with engine.begin() as conn:
        for row in data:
            ins = insert(table_obj).values(**row)
            update_dict = {c: ins.inserted[c] for c in updatable_cols}
            stmt = ins.on_duplicate_key_update(**update_dict)
            conn.execute(stmt)

# ------------------------------------------------------------
# 批次整表覆蓋（可選）：用 pandas.to_sql
#   df: pandas.DataFrame；mode: "replace" / "append"
# ------------------------------------------------------------
def upload_dataframe(table_name: str, df, mode: str = "replace") -> None:
    from sqlalchemy import text as _text  # 延遲載入避免不必要相依
    engine = create_engine(_mysql_url(), pool_pre_ping=True)
    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists=mode, index=False)
