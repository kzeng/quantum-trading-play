

import requests
from datetime import datetime, timezone
import pandas as pd


API_TOKEN = "cdce1f21dccb454b9c8b130a17af35b3f8a6ae87b9e8477d8069e9aa0d72e31f"  # 替换为你的 token
API_TOKEN = "cdce1f21dccb454b9c8b130a17af35b3f8a6ae87b9e8477d8069e9aa0d72e31f"  # replace with your own token

# Stock code list
scocket_code_list = ["600519"]  # Kweichow Moutai

url = "https://api.itick.org/stock/kline"


def fetch_min_kline(code: str, limit: int = 1000, end_ts_ms: int = None):
    """Fetch the latest `limit` 1-minute K-line bars from itick.

    Optionally specify the end timestamp `et` in milliseconds.
    """
    params = {
        "region": "SH",
        "code": code,
        "kType": 1,  # 1 = 1-minute K-line
        "limit": limit,
    }
    if end_ts_ms is not None:
        params["et"] = int(end_ts_ms)
    headers = {
        "accept": "application/json",
        "token": API_TOKEN,
    }

    resp = requests.get(url, params=params, headers=headers)
    data = resp.json()

    if data.get("code") != 0:
        print(f"{code} request failed: {data}")
        return []

    return data.get("data", [])


def fetch_min_kline_range(code: str, start_dt: datetime, end_dt: datetime, limit_per_request: int = 1000):
    """Fetch minute bars for a time range using repeated et+limit pagination.

    Continues paging backward until the [start_dt, end_dt] range is fully covered.
    """

    # Use UTC to compute timestamps (API docs annotate t as UTC)
    start_ts_ms = int(start_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts_ms = int(end_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    all_bars = []
    current_end = end_ts_ms

    while True:
        batch = fetch_min_kline(code, limit=limit_per_request, end_ts_ms=current_end)
        if not batch:
            break

        all_bars.extend(batch)
        ts_values = [b["t"] for b in batch]
        min_t = min(ts_values)

        # If this batch already reaches the start, or count < limit, no earlier data exists
        if min_t <= start_ts_ms or len(batch) < limit_per_request:
            break

        # Next page backwards
        current_end = min_t - 1

    # Filter to target time range and de-duplicate by timestamp
    filtered = [b for b in all_bars if start_ts_ms <= b["t"] <= end_ts_ms]
    if not filtered:
        return []

    filtered.sort(key=lambda x: x["t"])
    dedup = []
    last_t = None
    for b in filtered:
        if b["t"] != last_t:
            dedup.append(b)
            last_t = b["t"]

    return dedup


def convert_to_df(bars):
    """Convert itick response data into a pandas DataFrame."""
    rows = []
    for bar in bars:
        ts = bar["t"]
        # Detect whether the timestamp is in seconds or milliseconds
        if ts > 1e12:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts)

        rows.append(
            {
                "datetime": dt,
                "open": float(bar["o"]),
                "high": float(bar.get("h", bar["o"])),
                "low": float(bar.get("l", bar["o"])),
                "close": float(bar["c"]),
                "volume": float(bar.get("v", 0)),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def main():
    for scocket_code in scocket_code_list:
        print(f"\nStart processing stock: {scocket_code}")

        # First try to fetch full-year minute data for 2025
        start_2025 = datetime(2025, 1, 1, 0, 0, 0)
        end_2025 = datetime(2025, 12, 31, 23, 59, 59)
        print("Fetching 2025 minute data...")
        bars_2025 = fetch_min_kline_range(scocket_code, start_2025, end_2025, limit_per_request=1000)

        if not bars_2025:
            print(f"{scocket_code} has no minute data for 2025 (API returned no data or access is restricted)")
        else:
            df_2025 = convert_to_df(bars_2025)
            file_2025 = f"{scocket_code}_2025_mins.csv"
            df_2025.to_csv(file_2025, index=False)
            print(f"{scocket_code} 2025 minute data saved to: {file_2025} ({len(df_2025)} rows)")

        # Then fetch recent (near real-time) minute data and split by actual year
        print("\nFetching the most recent block of minute data...")
        bars_recent = fetch_min_kline(scocket_code, limit=100000)
        df_recent = convert_to_df(bars_recent)

        if df_recent.empty:
            print(f"{scocket_code} has no recent minute data")
            continue

        available_years = sorted(df_recent["datetime"].dt.year.unique())
        print(f"{scocket_code} recent data actually contains years: {available_years}")

        for year_int in available_years:
            df_year = df_recent[df_recent["datetime"].dt.year == year_int]
            year = str(year_int)

            file_name = f"{scocket_code}_{year}_mins.csv"
            df_year.to_csv(file_name, index=False)
            print(f"{scocket_code} {year} minute data saved: {file_name} ({len(df_year)} rows)")


if __name__ == "__main__":
    main()