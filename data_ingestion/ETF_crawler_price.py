# -*- coding: utf-8 -*-
# 抓取 TWSE ETF 歷史日價 → 直接 UPSERT 到 MySQL（逐月，簡單好讀）
# 變更：
# - 只抓到「當月月初」為止（不會打到未來）
# - 只有「出現第一個有資料的月份」之後，若連續 2 個月份 0 筆才提前停止（避免新 ETF 過早停）
# - 只在有資料時列印 [OK]，避免一堆 0 筆噪音

import re
import time
from datetime import datetime, date
from typing import List, Optional

import pandas as pd
import requests

from data_ingestion.mysql import init_db, upload_data_to_mysql_upsert, etf_day_price

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.twse.com.tw/",
}

# 全形轉半形（含簡易年月日處理）
FULL_TO_HALF = str.maketrans("０１２３４５６７８９／－年月日.", "0123456789/-   .")


def parse_twse_date(s: str) -> Optional[date]:
    """把 '2020/8/3' 或 '109/08/03' 轉成 date（容忍全形）。"""
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
        if y <= 300:  # 民國
            y += 1911
        return datetime.strptime(f"{y:04d}-{m:02d}-{d:02d}", "%Y-%m-%d").date()
    except Exception:
        return None


def to_float(x) -> Optional[float]:
    if x is None:
        return None
    s = str(x).replace(",", "").strip()
    if s in ("", "--"):
        return None
    try:
        return float(s)
    except Exception:
        return None


def to_int(x) -> Optional[int]:
    f = to_float(x)
    return int(f) if f is not None else None


def ETF_crawler_price(
    tickers: List[str],
    start: str,
    end: str,
    sleep: float = 0.7,
) -> None:
    init_db()
    sess = requests.Session()
    sess.headers.update(HEADERS)

    # ---- 上限鎖到「當月月初」----
    start_dt = datetime.strptime(start, "%Y%m%d").date()
    end_dt = datetime.strptime(end, "%Y%m%d").date()
    month_start_today = date.today().replace(day=1)
    end_cap = min(end_dt, month_start_today)

    months = pd.date_range(start=start_dt, end=end_cap, freq="MS")

    for tk in tickers:
        batch = []
        empty_streak = 0           # 連續空月份計數
        seen_any_data = False      # 是否已經遇到第一個有資料的月份

        for d in months:
            yyyymm01 = d.strftime("%Y%m01")
            url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?date={yyyymm01}&stockNo={tk}&response=json"
            try:
                resp = sess.get(url, timeout=15)
                resp.raise_for_status()
                rows = resp.json().get("data") or []
            except Exception as e:
                print(f"[WARN] {tk} {yyyymm01} 讀取失敗：{e}")
                time.sleep(sleep)
                continue

            # 0日期,1成交股數,2成交金額,3開盤價,4最高價,5最低價,6收盤價,7漲跌,8成交筆數
            got = 0
            for r in rows:
                if not isinstance(r, (list, tuple)) or len(r) < 9:
                    continue
                dt = parse_twse_date(r[0])
                cls = to_float(r[6])
                if not (dt and cls is not None):
                    continue  # 缺價就略過
                batch.append(
                    {
                        "ticker": tk,
                        "trade_date": dt,
                        "volume": to_int(r[1]),
                        "amount": to_int(r[2]),
                        "open": to_float(r[3]),
                        "high": to_float(r[4]),
                        "low": to_float(r[5]),
                        "close": cls,
                        "trades": to_int(r[8]),
                    }
                )
                got += 1

            if got > 0:
                print(f"[OK] {tk} {yyyymm01} 抓到 {got} 筆")
                seen_any_data = True
                empty_streak = 0
            else:
                # 尚未遇到第一個有資料月份 → 不要提早停（新 ETF 上市前的月份會都是 0）
                if seen_any_data:
                    empty_streak += 1
                    # 只有在 seen_any_data=True 之後，連續 2 個月 0 筆才提前停止
                    if empty_streak >= 2:
                        break

            time.sleep(sleep)

        if batch:
            upload_data_to_mysql_upsert(etf_day_price, batch)
            print(f"[DONE] {tk} 總上傳 {len(batch)} 筆")


if __name__ == "__main__":
    ETF_crawler_price(
        tickers=["0050", "00881", "00922", "00692", "00850", "0056", "00878", "00919", "00713", "00929"],
        start="20200101",
        end="21001231",  # 寫多無妨，會自動鎖到當月
        sleep=0.7,
    )
