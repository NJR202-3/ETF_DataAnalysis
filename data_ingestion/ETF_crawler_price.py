# -*- coding: utf-8 -*-
# ETF_crawler_price.py
# 功能：抓取 TWSE ETF 歷史「日收盤價」→ 直接 UPSERT 到 MySQL（逐月拉）

import time
import requests
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict

import pandas as pd

from data_ingestion.mysql import init_db, upload_data_to_mysql_upsert, etf_day_price

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.twse.com.tw/",
}

def parse_twse_date(s: str) -> date:
    """
    解析 TWSE 回傳日期：
      - '2020/8/3'（西元）
      - '109/08/03'（民國）
    皆轉成 datetime.date。
    """
    s = s.strip()
    if "年" in s:  # 很少見在這個 API，但仍防呆
        s = s.replace("年", "/").replace("月", "/").replace("日", "")
    y, m, d = [x.strip() for x in s.split("/")]
    y = int(y)
    if y <= 300:  # 視為民國
        y += 1911
    return datetime.strptime(f"{y:04d}-{int(m):02d}-{int(d):02d}", "%Y-%m-%d").date()

def to_int(x) -> int | None:
    if x is None:
        return None
    s = str(x).replace(",", "").strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None

def to_decimal(x) -> Decimal | None:
    if x is None:
        return None
    s = str(x).replace(",", "").strip()
    if s == "" or s == "--":
        return None
    try:
        return Decimal(s)
    except Exception:
        return None

def ETF_crawler_price(
    tickers: List[str],
    start: str,
    end: str,
    sleep: float = 0.7,
    headers: Dict[str, str] = HEADERS,
) -> None:
    """
    直接上傳到 MySQL 的日價爬蟲（不輸出 CSV）
    - 逐月拉 STOCK_DAY，合併後 UPSERT
    - start/end: 'YYYYMMDD'（會以每月一號為粒度）
    """
    init_db()
    sess = requests.Session()
    sess.headers.update(headers)

    start_dt = datetime.strptime(start, "%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")
    months = pd.date_range(start=start_dt, end=end_dt, freq="MS")  # 每月一號

    for tk in tickers:
        batch = []
        for d in months:
            yyyymm01 = d.strftime("%Y%m01")
            url = (
                "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
                f"?date={yyyymm01}&stockNo={tk}&response=json"
            )
            try:
                r = sess.get(url, timeout=15)
                r.raise_for_status()
                j = r.json()
            except Exception as e:
                print(f"[WARN] {tk} {yyyymm01} 讀取失敗：{e}")
                time.sleep(sleep)
                continue

            rows = j.get("data") or []
            for row in rows:
                # 欄位順序（通常）：
                # 0日期,1成交股數,2成交金額,3開盤價,4最高價,5最低價,6收盤價,7漲跌,8成交筆數
                if not isinstance(row, (list, tuple)) or len(row) < 9:
                    continue
                batch.append(
                    {
                        "ticker": tk,
                        "trade_date": parse_twse_date(row[0]),
                        "volume": to_int(row[1]),
                        "amount": to_int(row[2]),
                        "open": to_decimal(row[3]),
                        "high": to_decimal(row[4]),
                        "low": to_decimal(row[5]),
                        "close": to_decimal(row[6]),
                        "trades": to_int(row[8]),
                    }
                )

            print(f"[OK] {tk} {yyyymm01} 抓到 {len(rows)} 筆")
            time.sleep(sleep)

        if batch:
            upload_data_to_mysql_upsert(etf_day_price, batch)
            print(f"[DONE] {tk} 總上傳 {len(batch)} 筆")

if __name__ == "__main__":
    ETF_crawler_price(
        tickers=["0050", "0051", "0052", "0056", "00713", "00878"],
        start="20200701",   # 月粒度，寫當月 1 號最安全
        end="20250901",
        sleep=0.7,
    )



