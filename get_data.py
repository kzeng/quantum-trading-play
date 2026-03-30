import baostock as bs
import pandas as pd
import requests
from datetime import datetime, timezone


API_TOKEN = "cdce1f21dccb454b9c8b130a17af35b3f8a6ae87b9e8477d8069e9aa0d72e31f"
API_URL = "https://api.itick.org/stock/kline"


# define stock code list
# scocket_code_list = ["600519"]  # 贵州茅台
scocket_code_list = ["000988"]  # 华工科技


# define date range list (start_date, end_date)
date_range_list = [
    # ("2020-01-01", "2020-12-31"),
    # ("2021-01-01", "2021-12-31"),
    # ("2022-01-01", "2022-12-31"),
    # ("2023-01-01", "2023-12-31"),
    # ("2024-01-01", "2024-12-31"),
    # ("2025-01-01", "2025-12-31"),
    ("2026-01-01", "2026-12-31"),
]


def fetch_daily_kline_itick(code: str, region: str, limit: int = 1000, end_ts_ms: int | None = None):
    """Fetch the most recent `limit` daily K-line bars from itick.

    Optionally specify the end timestamp `et` in milliseconds. `kType=8` means daily bars.
    """

    params: dict[str, object] = {
        "region": region,
        "code": code,
        "kType": 8,  # 8 = one day
        "limit": limit,
    }
    if end_ts_ms is not None:
        params["et"] = int(end_ts_ms)

    headers = {
        "accept": "application/json",
        "token": API_TOKEN,
    }

    try:
        resp = requests.get(API_URL, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        print("[itick] Daily K-line request timed out, skipping this round")
        return []
    except requests.exceptions.RequestException as exc:
        print(f"[itick] Daily K-line request error: {exc}")
        return []

    data = resp.json()
    if data.get("code") != 0:
        print(f"[itick] Daily K-line response error: {data}")
        return []

    return data.get("data", []) or []


def fetch_daily_kline_range_itick(code: str, region: str, start_dt: datetime, end_dt: datetime, limit_per_request: int = 1000) -> pd.DataFrame:
    """Fetch all daily K-line bars between [start_dt, end_dt] via multiple requests.

    Returns a DataFrame in the same format as 000988_2026.csv.
    """

    start_ts_ms = int(start_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ts_ms = int(end_dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

    all_bars: list[dict] = []
    current_end = end_ts_ms

    while True:
        batch = fetch_daily_kline_itick(code, region=region, limit=limit_per_request, end_ts_ms=current_end)
        if not batch:
            break

        all_bars.extend(batch)
        ts_values = [b["t"] for b in batch]
        min_t = min(ts_values)

        if min_t <= start_ts_ms or len(batch) < limit_per_request:
            break

        current_end = min_t - 1

    if not all_bars:
        return pd.DataFrame()

    # Filter bars to the target time range
    filtered = [b for b in all_bars if start_ts_ms <= b["t"] <= end_ts_ms]
    if not filtered:
        return pd.DataFrame()

    rows: list[dict] = []
    for bar in filtered:
        ts = bar["t"]
        if ts > 1e12:
            ts = ts / 1000.0
        dt = datetime.fromtimestamp(ts).date()

        rows.append(
            {
                "date": dt,  # use date here and unify with the Baostock workflow later
                "open": float(bar["o"]),
                "high": float(bar.get("h", bar["o"])),
                "low": float(bar.get("l", bar["o"])),
                "close": float(bar["c"]),
                "volume": float(bar.get("v", 0)),
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# Log in to Baostock
lg = bs.login()
print("Login succeeded")

for scocket_code in scocket_code_list:
    if scocket_code.startswith("6"):
        full_code = f"sh.{scocket_code}"
    elif scocket_code.startswith("0") or scocket_code.startswith("3"):
        full_code = f"sz.{scocket_code}"
    else:
        full_code = f"sh.{scocket_code}"  # currently assume it is an SSE stock

    for start_date, end_date in date_range_list:
        print(f"\nStart fetching daily bars for {full_code} from {start_date} to {end_date}...")

        rs = bs.query_history_k_data_plus(
            full_code,
            "date,open,high,low,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",  # 2 = forward-adjusted prices
        )

        # Print query result status
        print("Query return code:", rs.error_code)
        print("Query return message:", rs.error_msg)

        # Convert to DataFrame
        data_list: list[list[str]] = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if data_list:
            # Use Baostock returned data
            df = pd.DataFrame(data_list, columns=rs.fields)
        else:
            # Baostock returned no data: try generating daily bars from itick
            print("Baostock returned no data, trying to generate daily bars from itick...")

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

            # itick uses codes without exchange prefix plus a region value
            region = "SH" if scocket_code.startswith("6") else "SZ"
            df = fetch_daily_kline_range_itick(scocket_code, region=region, start_dt=start_dt, end_dt=end_dt)

            if df.empty:
                print("itick daily K-line also failed to fetch data for this range, skipping.")
                continue

        # Cast data types
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["date"] = pd.to_datetime(df["date"])

        # Rename columns to match backtrader
        df.rename(
            columns={
                "date": "datetime",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            },
            inplace=True,
        )

        # Sort by date
        df.sort_values("datetime", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Inspect data
        print("\nData preview:")
        print(df.head())
        print(f"\nTotal rows fetched: {len(df)}")
        print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")

        # Generate file name based on the year in start_date
        year = start_date[:4]
        file_name = f"{scocket_code}_{year}.csv"

        # Save to local CSV
        df.to_csv("./data/"+file_name, index=False)
        print(f"\nData saved to {file_name}")

# Log out of Baostock
bs.logout()
print("Logout succeeded")