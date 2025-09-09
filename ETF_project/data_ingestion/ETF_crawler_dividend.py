# -*- coding: utf-8 -*-
# ETF_crawler_dividend.py
# 功能：抓取 TWSE ETF 歷年配息 → 直接 UPSERT 到 MySQL

import time
import requests
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict

from data_ingestion.mysql import init_db, upload_data_to_mysql_upsert, etf_dividend

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.twse.com.tw/",
}

def parse_tw_date_chinese(s: str) -> date:
    """
    轉中文民國日期為 date，例如：
      '114年07月21日' -> 2025-07-21
    """
    s = s.replace("年", "/").replace("月", "/").replace("日", "")
    y, m, d = s.split("/")
    ce = int(y) + 1911
    return datetime.strptime(f"{ce:04d}-{int(m):02d}-{int(d):02d}", "%Y-%m-%d").date()

def to_decimal(x) -> Decimal | None:
    if x is None:
        return None
    s = str(x).replace(",", "").strip()
    if s == "":
        return None
    try:
        return Decimal(s)
    except Exception:
        return None

def ETF_crawler_dividend(
    tickers: List[str],
    start: str,
    end: str,
    sleep: float = 0.7,
    headers: Dict[str, str] = HEADERS,
) -> None:
    """
    直接上傳到 MySQL 的配息爬蟲（不輸出 CSV）
    - tickers: 例如 ["0050","0051",...]
    - start:   'YYYYMMDD'（TWSE API 需要）
    - end:     'YYYYMMDD'
    """
    init_db()
    sess = requests.Session()
    sess.headers.update(headers)

    for tk in tickers:
        url = (
            "https://www.twse.com.tw/rwd/zh/ETF/etfDiv"
            f"?stkNo={tk}&startDate={start}&endDate={end}&response=json"
        )
        try:
            r = sess.get(url, timeout=15)
            r.raise_for_status()
            j = r.json()
        except Exception as e:
            print(f"[WARN] {tk} 讀取失敗：{e}")
            time.sleep(sleep)
            continue

        data = j.get("data") or []
        rows = []
        for row in data:
            # 回傳格式： [代號, 簡稱, 除息交易日, 基準日, 發放日, 金額]
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            rows.append(
                {
                    "ticker": row[0],
                    "short_name": row[1],
                    "ex_date": parse_tw_date_chinese(row[2]),
                    "record_date": parse_tw_date_chinese(row[3]),
                    "payable_date": parse_tw_date_chinese(row[4]),
                    "cash_dividend": to_decimal(row[5]),
                }
            )

        if rows:
            upload_data_to_mysql_upsert(etf_dividend, rows)
        print(f"[OK] {tk} 配息筆數：{len(rows)}")
        time.sleep(sleep)

if __name__ == "__main__":
    # 固定參數直接寫在這裡
    ETF_crawler_dividend(
        tickers=["0050", "0051", "0052", "0056", "00713", "00878"],
        start="20200720",   # 從 00878 上市日開始，其他標的也會覆蓋到
        end="21001231",
        sleep=0.7,
    )




