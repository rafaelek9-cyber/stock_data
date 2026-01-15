import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime
import os

# ---------------- SETUP ---------------- #

os.makedirs("data", exist_ok=True)

DATA_PATH = "data/stock_data.xlsx"
TIMEZONE = pytz.timezone("US/Eastern")


# ---------------- TIME SETUP ---------------- #

def generate_time_slots():
    return pd.date_range(
        start="06:00",
        end="14:00",
        freq="15min"
    ).strftime("%H:%M").tolist()


TIMES = generate_time_slots()


# ---------------- COLUMN SETUP ---------------- #

def build_columns(times):
    cols = ["Ticker"]

    for i, t in enumerate(times):
        cols.append(f"{t} Price")
        if i > 0:
            cols.append(f"% {times[i-1]}→{t}")

    cols += ["TOTAL % 6:00→2:00", "Momentum_3x", "Momentum_Break"]
    return cols


# ---------------- TOP GAINERS ---------------- #

def get_top_20_gainers():
    tickers = [
        "AAPL", "TSLA", "AMD", "NVDA", "META", "AMZN", "MSFT",
        "PLTR", "RIVN", "COIN", "SOFI", "F", "BAC", "INTC",
        "MU", "AI", "DKNG", "SNAP", "UPST", "AFRM"
    ]

    data = []

    for t in tickers:
        try:
            hist = yf.Ticker(t).history(
                period="1d",
                interval="15m",
                prepost=True
            )
            if len(hist) >= 2:
                pct = (
                    (hist["Close"].iloc[-1] - hist["Open"].iloc[0])
                    / hist["Open"].iloc[0]
                ) * 100
                data.append((t, pct))
        except Exception:
            continue

    df = pd.DataFrame(data, columns=["Ticker", "Pct"])
    return df.sort_values("Pct", ascending=False).head(20)["Ticker"].tolist()


# ---------------- LOAD / INIT DATAFRAME ---------------- #
def load_or_create_df():
    os.makedirs("data", exist_ok=True)

    # ALWAYS create a base file if it doesn't exist
    if not os.path.exists(DATA_PATH):
        tickers = get_top_20_gainers()

        if not tickers:
            # Emergency fallback to prevent empty file
            tickers = ["AAPL", "MSFT", "NVDA"]

        df = pd.DataFrame({"Ticker": tickers})

        for col in build_columns(TIMES):
            if col != "Ticker":
                df[col] = None

        df.to_excel(DATA_PATH, index=False)
        print("✅ Excel file CREATED")
        return df

    print("ℹ️ Excel file already exists")
    return pd.read_excel(DATA_PATH)
# ---------------- UPDATE PRICES ---------------- #

def update_prices(df, current_time):
    idx = TIMES.index(current_time)

    for i, row in df.iterrows():
        ticker = row["Ticker"]

        try:
            hist = yf.download(
                ticker,
                interval="15m",
                period="1d",
                prepost=True,
                progress=False
            )
        except Exception as e:
            print(f"Download failed for {ticker}: {e}")
            continue

        if hist is None or hist.empty:
            continue

        price = hist["Close"].iloc[-1]
        df.at[i, f"{current_time} Price"] = price

        # % change vs previous 15-min interval
        if idx > 0:
            prev_time = TIMES[idx - 1]
            prev_price = row.get(f"{prev_time} Price")

            if pd.notna(prev_price) and prev_price > 0:
                pct = (price - prev_price) / prev_price * 100
                df.at[i, f"% {prev_time}→{current_time}"] = round(pct, 2)

        # Total % change from 6:00
        open_price = row.get("06:00 Price")
        if pd.notna(open_price) and open_price > 0:
            total = (price - open_price) / open_price * 100
            df.at[i, "TOTAL % 6:00→2:00"] = round(total, 2)