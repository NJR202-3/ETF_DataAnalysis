from datetime import datetime, date
from data_ingestion.mysql import init_db, upload_data_to_mysql_upsert, etf_day_price
import requests
import time
import pandas as pd


# 日期轉換格式的funtion
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

# 抓每天日收盤資訊，每天下午2點去跑排程並加入
def ETF_crawler_today_price(t=["0050", "0051", "0052", "0056", "00713", "00878"]):
    tickers=t
    total_df = pd.DataFrame()
    for ticker in tickers:
        today = datetime.today().strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={today}&stockNo={ticker}"
        col_name = ["trade_date", "volume", "amount", "open", "high", "low", "close", "漲跌價差", "trades"]

        req = requests.get(url)
        json_data = req.json()

        # 防呆檢查
        if "data" not in json_data:
            print(f"⚠️ {today} 沒有資料，回傳內容：{json_data.get('stat', '無 stat 訊息')}")
            continue
        df = pd.DataFrame([json_data["data"][-1]], columns=col_name)

        # 每個月資料跑出來後就先加上一個股票代號的欄位在最前面
        df.insert(0, "ticker", ticker)
        # 刪除不必要的欄位
        df = df.drop(columns="漲跌價差")
        # 重新改日期格式
        df["trade_date"] = df["trade_date"].apply(parse_twse_date)
        # 把volume、amount、trades的逗號刪除
        df["volume"] = df["volume"].str.replace(",", "").astype(int)
        df["amount"] = df["amount"].str.replace(",", "").astype(int)
        df["trades"] = df["trades"].str.replace(",", "").astype(int)

        # 把撈出來的每個月股價資料合併在total_df裡面
        total_df = pd.concat([total_df, df], ignore_index=True)
        time.sleep(10)
    
    total_dict = total_df.to_dict(orient="records")
    # 上傳今日ETF資料到MySQL
    upload_data_to_mysql_upsert(etf_day_price, total_dict)
    print(f"[DONE] 總上傳 {len(total_df)} 筆")

if __name__ == "__main__":
    ETF_crawler_today_price()