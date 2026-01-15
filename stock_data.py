import os
import yfinance as yf
import pandas as pd
from datetime import datetime, time

# =========================
# CONFIG
# =========================

DATA_PATH = "data/stock_data.xlsx"

TIMES = [
    time(7, 0),
    time(7, 15),
    time(7, 30),
    time(7, 45),
    time(8, 0),
    time(8, 15),
    time(8, 30),
    time(8, 45),
    time(9, 0),
    time(9, 15),
    time(9, 30),
    time(9, 45),
    time(10, 0),
    time(10, 15),
    time(10, 30),
    time(10, 45),
    time(11, 0),
    time(11, 15),
    time(11, 30),
    time(11, 45),
    time(12, 0),
]

# =========================
# HELPERS
# =========================

def build_columns(times):
    cols = ["Ticker", "7:00 Price"]

    for t in times[1:]:
        label = t.strftime("%H:%M")
        cols.append(f"{label} Price")
        cols.append(f"{label} % Change")

    cols.append("Total % Change")
    return cols


def get_top_20_gainers():
    """
    Placeholder for premarket gainer logic.
    MUST return a list or the file will fallback safely.
    """
    return ["AAPL", "MSFT", "NVDA", "TSLA", "AMD"]


def get_price(ticker):
    try:
        data = yf.Ticker(ticker).history(period="1d", interval="1m")
        if data.empty:
            return None
        return round(data["Close"].iloc[-1], 2)
    except Exception as e:
        print(f"Price fetch failed for {ticker}: {e}")
        return None


# =========================
# FILE HANDLING
# =========================

def load_or_create_df():
    os.makedirs("data", exist_ok=True)

    if os.path.exists(DATA_PATH):
        try:
            # 🚨 Force engine AND validate file
            df = pd.read_excel(DATA_PATH, engine="openpyxl")

            if df.empty:
                raise ValueError("Excel file is empty")

            return df

        except Exception as e:
            print(f"⚠️ Excel file corrupted — rebuilding: {e}")
            os.remove(DATA_PATH)

    # -------- CREATE FRESH FILE --------

    print("📄 Creating new Excel file")

    tickers = get_top_20_gainers()
    if not tickers:
        tickers = ["AAPL", "MSFT", "NVDA"]

    df = pd.DataFrame({"Ticker": tickers})

    for col in build_columns(TIMES):
        if col != "Ticker":
            df[col] = None

    # 🚨 FORCE REAL XLSX WRITE
    df.to_excel(DATA_PATH, index=False, engine="openpyxl")

    print("✅ Excel file created cleanly")
    return df
# =========================
# DATA UPDATES
# =========================

def update_prices(df, now):
    base_col = "7:00 Price"

    for idx, row in df.iterrows():
        ticker = row["Ticker"]
        price = get_price(ticker)

        if price is None:
            continue

        if pd.isna(row[base_col]):
            df.at[idx, base_col] = price
            continue

        label = now.strftime("%H:%M")
        price_col = f"{label} Price"
        pct_col = f"{label} % Change"

        if price_col not in df.columns:
            return

        base_price = row[base_col]
        pct_change = round(((price - base_price) / base_price) * 100, 2)

        df.at[idx, price_col] = price
        df.at[idx, pct_col] = pct_change


def update_total_change(df):
    for idx, row in df.iterrows():
        base = row["7:00 Price"]

        if pd.isna(base):
            continue

        latest_price = base

        for t in reversed(TIMES):
            col = f"{t.strftime('%H:%M')} Price"
            if col in df.columns and not pd.isna(row[col]):
                latest_price = row[col]
                break

        total_change = round(((latest_price - base) / base) * 100, 2)
        df.at[idx, "Total % Change"] = total_change


# =========================
# MAIN
# =========================

def main():
    now = datetime.now().time()
    valid_times = [t for t in TIMES if t <= now]

    if not valid_times:
        print("⏳ Before tracking window — exiting safely")
        return

    now = max(valid_times)

    df = load_or_create_df()
    update_prices(df, now)
    update_total_change(df)

    os.makedirs("data", exist_ok=True)
    df.to_excel(DATA_PATH, index=False)

    assert os.path.exists(DATA_PATH), "❌ Excel file was NOT created"

    print(f"✅ Excel saved: {DATA_PATH}")
    print(f"⏱ Updated at {now.strftime('%H:%M')}")


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    main()