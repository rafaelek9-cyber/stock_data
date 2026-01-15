import os
import pandas as pd
import yfinance as yf
from datetime import datetime, time
import openpyxl

DATA_PATH = "data/stock_data.xlsx"

TIMES = [
    "06:00", "06:15", "06:30", "06:45",
    "07:00", "07:15", "07:30", "07:45",
    "08:00", "08:15", "08:30", "08:45",
    "09:00", "09:15", "09:30", "09:45",
    "10:00", "10:15", "10:30", "10:45",
    "11:00", "11:15", "11:30", "11:45",
    "12:00", "12:15", "12:30", "12:45",
    "13:00", "13:15", "13:30", "13:45",
    "14:00"
]


def build_columns(times):
    cols = ["Ticker"]
    for t in times:
        cols.append(f"Price {t}")
        cols.append(f"% Δ prev → {t}")
        cols.append(f"Momentum {t}")
    cols.append("FINAL PRICE")
    cols.append("TOTAL % 6:00→2:00")
    return cols


def get_top_20_gainers():
    try:
        tickers = yf.Tickers(" ".join([
            "AAPL", "MSFT", "NVDA", "AMD", "TSLA", "META", "AMZN",
            "COIN", "RIOT", "MARA", "SMCI", "PLTR", "SOFI", "AI",
            "UPST", "CVNA", "AFRM", "RBLX", "DKNG", "SHOP"
        ]))
        return list(tickers.tickers.keys())[:20]
    except Exception:
        return []


def load_or_create_df():
    os.makedirs("data", exist_ok=True)

    if os.path.exists(DATA_PATH):
        try:
            df = pd.read_excel(DATA_PATH, engine="openpyxl")
            if not df.empty:
                return df
        except Exception:
            os.remove(DATA_PATH)

    tickers = get_top_20_gainers()
    if not tickers:
        tickers = ["AAPL", "MSFT", "NVDA"]

    df = pd.DataFrame({"Ticker": tickers})

    for col in build_columns(TIMES):
        if col != "Ticker":
            df[col] = None

    df.to_excel(DATA_PATH, index=False, engine="openpyxl")
    return df


def get_price(ticker):
    try:
        data = yf.download(
            ticker,
            period="1d",
            interval="1m",
            progress=False,
            prepost=True
        )
        return float(data["Close"].iloc[-1])
    except Exception:
        return None


def momentum_flag(pct):
    if pct is None:
        return ""
    if pct > 2:
        return "🚀 STRONG"
    if pct > 0.5:
        return "⬆️ UP"
    if pct < -2:
        return "🔻 STRONG DOWN"
    if pct < -0.5:
        return "⬇️ DOWN"
    return "➖ FLAT"


def main():
    now = datetime.now().strftime("%H:%M")
    if now not in TIMES:
        return

    df = load_or_create_df()

    for i, ticker in enumerate(df["Ticker"]):
        price = get_price(ticker)
        if price is None:
            continue

        price_col = f"Price {now}"
        pct_col = f"% Δ prev → {now}"
        mom_col = f"Momentum {now}"

        prev_time = TIMES[TIMES.index(now) - 1] if TIMES.index(now) > 0 else None
        prev_price = df.at[i, f"Price {prev_time}"] if prev_time else None

        df.at[i, price_col] = round(price, 2)

        pct = None
        if prev_price:
            pct = ((price - prev_price) / prev_price) * 100
            df.at[i, pct_col] = round(pct, 2)

        df.at[i, mom_col] = momentum_flag(pct)

        if now == "14:00":
            open_price = df.at[i, "Price 06:00"]
            if open_price:
                total = ((price - open_price) / open_price) * 100
                df.at[i, "FINAL PRICE"] = round(price, 2)
                df.at[i, "TOTAL % 6:00→2:00"] = round(total, 2)

    df.to_excel(DATA_PATH, index=False, engine="openpyxl")


if __name__ == "__main__":
    main()