import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime
import os

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


# ---------------- TOP GAINERS (6:30) ---------------- #

def get_top_20_gainers():
    tickers = [
        "AAPL", "TSLA", "AMD", "NVDA", "META", "AMZN", "MSFT",
        "PLTR", "RIVN", "COIN", "SOFI", "F", "BAC", "INTC",
        "MU", "AI", "DKNG", "SNAP", "UPST", "AFRM"
    ]

    data = []

    for t in tickers:
        try:
            hist = yf.Ticker(t).history(period="1d", interval="15m", prepost=True)
            if len(hist) >= 2:
                pct = (hist["Close"].iloc[-1] - hist["Open"].iloc[0]) / hist["Open"].iloc[0] * 100
                data.append((t, pct))
        except:
            continue

    df = pd.DataFrame(data, columns=["Ticker", "Pct"])
    return df.sort_values("Pct", ascending=False).head(20)["Ticker"].tolist()


# ---------------- LOAD / INIT DATAFRAME ---------------- #

def load_or_create_df():
    os.makedirs("data", exist_ok=True)

    if os.path.exists(DATA_PATH):
        return pd.read_excel(DATA_PATH)

    tickers = get_top_20_gainers()
    df = pd.DataFrame({"Ticker": tickers})

    for col in build_columns(TIMES):
        if col != "Ticker":
            df[col] = None

    # 🚨 FORCE FILE CREATION IMMEDIATELY
    df.to_excel(DATA_PATH, index=False)
    return df

# ---------------- UPDATE PRICES ---------------- #

def update_prices(df, current_time):
    idx = TIMES.index(current_time)

    for i, row in df.iterrows():
        ticker = row["Ticker"]
# --- DOWNLOAD INTRADAY DATA ---
hist = None

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
            continue

        df.at[i, f"{current_time} Price"] = price

        # Interval % change
        if idx > 0:
            prev_time = TIMES[idx - 1]
            prev_price = row.get(f"{prev_time} Price")

            if pd.notna(prev_price) and prev_price > 0:
                pct = (price - prev_price) / prev_price * 100
                df.at[i, f"% {prev_time}→{current_time}"] = round(pct, 2)

        # Total % change
        open_price = row.get("06:00 Price")
        if pd.notna(open_price) and open_price > 0:
            total = (price - open_price) / open_price * 100
            df.at[i, "TOTAL % 6:00→2:00"] = round(total, 2)


# ---------------- MOMENTUM FLAGS ---------------- #

def update_momentum_flags(df):
    for i, row in df.iterrows():
        pct_cols = [c for c in df.columns if c.startswith("%")]
        values = [row[c] for c in pct_cols if pd.notna(row[c])]

        momentum_3x = False
        momentum_break = False

        if len(values) >= 3:
            if values[-1] > 0 and values[-2] > 0 and values[-3] > 0:
                momentum_3x = True

        if len(values) >= 3:
            if values[-1] < 0 and values[-2] > 0 and values[-3] > 0:
                momentum_break = True

        df.at[i, "Momentum_3x"] = momentum_3x
        df.at[i, "Momentum_Break"] = momentum_break


# ---------------- MAIN ---------------- #

def main():
    now = datetime.now(TIMEZONE).strftime("%H:%M")

    if now not in TIMES:
        now = max(t for t in TIMES if t <= now)

    df = load_or_create_df()
    update_prices(df, now)
    update_momentum_flags(df)

    df.to_excel(DATA_PATH, index=False)
    print(f"Updated data for {now}")