import os
import pandas as pd
import yfinance as yf
from datetime import datetime
import openpyxl

DATA_PATH = "data/stock_data.xlsx"
TICKER_FILE = "tickers.txt"


def load_tickers():
    if not os.path.exists(TICKER_FILE):
        return ["AAPL", "MSFT", "NVDA"]

    with open(TICKER_FILE) as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    return tickers[:20]


def load_or_create_df(tickers):
    os.makedirs("data", exist_ok=True)

    columns = [
        "Ticker",
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "% Change"
    ]

    if os.path.exists(DATA_PATH):
        try:
            df = pd.read_excel(DATA_PATH, engine="openpyxl")
            if not df.empty:
                return df
        except Exception:
            os.remove(DATA_PATH)

    df = pd.DataFrame(columns=columns)
    df.to_excel(DATA_PATH, index=False, engine="openpyxl")
    return df


def get_eod_data(ticker):
    try:
        data = yf.download(
            ticker,
            period="1d",
            interval="1d",
            progress=False
        )

        if data.empty:
            return None

        row = data.iloc[-1]
        pct_change = ((row["Close"] - row["Open"]) / row["Open"]) * 100

        return {
            "Open": round(row["Open"], 2),
            "High": round(row["High"], 2),
            "Low": round(row["Low"], 2),
            "Close": round(row["Close"], 2),
            "Volume": int(row["Volume"]),
            "% Change": round(pct_change, 2)
        }

    except Exception:
        return None


def main():
    tickers = load_tickers()
    df = load_or_create_df(tickers)

    today = datetime.now().strftime("%Y-%m-%d")
    rows = []

    for ticker in tickers:
        data = get_eod_data(ticker)
        if not data:
            continue

        rows.append({
            "Ticker": ticker,
            "Date": today,
            **data
        })

    if rows:
        new_df = pd.DataFrame(rows)
        df = pd.concat([df, new_df], ignore_index=True)

    df.to_excel(DATA_PATH, index=False, engine="openpyxl")


if __name__ == "__main__":
    main()
