# -*- coding: utf-8 -*-
# ETF_crawler_dividend.py
import re
import time
import requests
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from data_ingestion.mysql import init_db, upload_data_to_mysql_upsert, etf_dividend

# 只轉全形數字與標點；不要轉「年/月/日」！
FULL_TO_HALF = str.maketrans("０１２３４５６７８９／－．", "0123456789/-.")
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.twse.com.tw/",
}

def parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip().translate(FULL_TO_HALF)
    s = s.replace("年", "/").replace("月", "/").replace("日", "")
    s = re.sub(r"[^\d/-]", "", s)
    parts = re.split(r"[/-]", s)
    if len(parts) != 3:
        return None
    try:
        y, m, d = map(int, parts)
        if y < 1911:  # 民國年
            y += 1911
        if not (1990 <= y <= 2100):
            return None
        return datetime.strptime(f"{y:04d}-{m:02d}-{d:02d}", "%Y-%m-%d").date()
    except Exception:
        return None

def parse_cash_anywhere(row: List) -> Optional[float]:
    """先用第 6 欄；若不是數字，就往後找第一個像金額的數字。"""
    def to_float(x):
        try:
            s = str(x).replace(",", "").strip()
            if s in ("", "--"):
                return None
            return float(s)
        except Exception:
            return None

    if len(row) > 5:
        v = to_float(row[5])
        if v is not None:
            return v

    for x in row[6:]:
        s = str(x).replace(",", "").strip()
        if re.fullmatch(r"-?\d+(\.\d+)?", s) and "." in s and len(s) <= 12:
            try:
                return float(s)
            except Exception:
                pass
    return None

def ETF_crawler_dividend(tickers: List[str], start: str, end: str, sleep: float = 0.7) -> None:
    init_db()
    sess = requests.Session()
    sess.headers.update(HEADERS)

    for tk in tickers:
        url = f"https://www.twse.com.tw/rwd/zh/ETF/etfDiv?stkNo={tk}&startDate={start}&endDate={end}&response=json"
        try:
            r = sess.get(url, timeout=15)
            r.raise_for_status()
            data = r.json().get("data") or []
        except Exception as e:
            print(f"[WARN] {tk} 讀取失敗：{e}")
            time.sleep(sleep)
            continue

        # 用 (ticker, ex_date) 彙總，避免同日多筆
        agg: Dict[Tuple[str, date], Dict] = {}

        for row in data:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue

            ex = parse_date(row[2])
            rd = parse_date(row[3])
            pd = parse_date(row[4])
            cash = parse_cash_anywhere(row)

            if not (ex and cash is not None):
                continue
            if rd is None:
                rd = ex
            if pd is None:
                pd = ex

            key = (tk, ex)
            if key not in agg:
                agg[key] = {
                    "ticker": tk,
                    "short_name": row[1],
                    "ex_date": ex,
                    "record_date": rd,
                    "payable_date": pd,
                    "cash_dividend": cash,
                }
            else:
                agg[key]["cash_dividend"] += cash

        rows = list(agg.values())
        if rows:
            upload_data_to_mysql_upsert(etf_dividend, rows)
        print(f"[OK] {tk} 配息筆數（彙總後）：{len(rows)}")
        time.sleep(sleep)

if __name__ == "__main__":
    ETF_crawler_dividend(
        tickers=["0050","00881","00922","00692","00850","0056","00878","00919","00713","00929"],
        start="20200101", end="21001231", sleep=0.7,
    )
