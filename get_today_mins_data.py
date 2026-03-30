import os
import time
from datetime import date, datetime, time as dtime, timedelta

import pandas as pd
import requests


API_TOKEN = "cdce1f21dccb454b9c8b130a17af35b3f8a6ae87b9e8477d8069e9aa0d72e31f"

API_URL = "https://api.itick.org/stock/kline"
STOCK_CODE = "000988"  # HGTECH


def fetch_recent_min_kline(code: str, region: str, limit: int = 2000, end_ts_ms: int | None = None):
    """Fetch recent 1-minute kline data from itick.

    :param code: Stock code, e.g. "000988"
    :param region: Exchange region, e.g. "SZ" or "SH"
    :param limit: Max number of bars to return
    :param end_ts_ms: Optional end timestamp in milliseconds
    :return: List of bar dicts from API
    """
    params: dict[str, object] = {
        "region": region,
        "code": code,
        "kType": 1,  # 1-minute kline
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
        print("请求 itick 接口超时，本次跳过，等待下一分钟...")
        return []
    except requests.exceptions.RequestException as exc:
        print(f"请求 itick 接口异常: {exc}，本次跳过。")
        return []

    data = resp.json()

    if data.get("code") != 0:
        print(f"{code} 请求失败: {data}")
        return []

    return data.get("data", []) or []


def convert_to_df(bars: list[dict]) -> pd.DataFrame:
    """Convert itick bar list to pandas DataFrame.

    Columns: datetime, open, high, low, close, volume
    """
    rows: list[dict] = []
    for bar in bars:
        ts = bar["t"]
        # detect seconds vs milliseconds
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


def get_prev_close_from_csv(code: str, data_dir: str = "data") -> float | None:
    """Get the most recent trading day's close from local daily CSVs.

    Used as the reference price for percentage change. Assumes the data
    directory already contains daily files such as 000988_2020.csv ~ 000988_2026.csv.
    """

    if not os.path.isdir(data_dir):
        return None

    today = date.today()
    last_trade_date: date | None = None
    last_close: float | None = None

    for fname in os.listdir(data_dir):
        if not fname.startswith(f"{code}_"):
            continue
        if "mins" in fname:
            # Skip minute-level files
            continue

        path = os.path.join(data_dir, fname)
        try:
            df = pd.read_csv(path)
        except Exception:
            continue

        if "datetime" not in df.columns or "close" not in df.columns:
            continue

        try:
            df["datetime"] = pd.to_datetime(df["datetime"])
        except Exception:
            continue

        # Only consider daily bars before today and pick the latest one
        df_prev = df[df["datetime"].dt.date < today]
        if df_prev.empty:
            continue

        row = df_prev.sort_values("datetime").iloc[-1]
        trade_date: date = row["datetime"].date()
        close_val = float(row["close"])

        if last_trade_date is None or trade_date > last_trade_date:
            last_trade_date = trade_date
            last_close = close_val

    return last_close


def is_trading_time(now: datetime) -> bool:
    """Return True if current time is within continuous trading hours.

    Does not include call auction; only continuous trading sessions.
    """

    t = now.time()
    morning_start = dtime(9, 30)
    morning_end = dtime(11, 30)
    afternoon_start = dtime(13, 0)
    afternoon_end = dtime(15, 0)

    in_morning = morning_start <= t <= morning_end
    in_afternoon = afternoon_start <= t <= afternoon_end
    return in_morning or in_afternoon


def is_after_close(now: datetime) -> bool:
    """Return True if market is already closed (after 15:00)."""

    return now.time() >= dtime(15, 0)


def sleep_until_next_minute() -> None:
    """Sleep until the next whole minute boundary."""

    now = datetime.now()
    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    delta = (next_minute - now).total_seconds()
    if delta > 0:
        time.sleep(delta)


def main() -> None:
    # Decide region by stock code prefix (same rule as get_data.py)
    if STOCK_CODE.startswith("6"):
        region = "SH"
    elif STOCK_CODE.startswith("0") or STOCK_CODE.startswith("3"):
        region = "SZ"
    else:
        region = "SH"

    today = date.today()
    print(f"Start processing per-minute data for {region}.{STOCK_CODE} on {today}...")

    # First try to get yesterday's close from local daily data as reference price
    prev_close = get_prev_close_from_csv(STOCK_CODE)
    if prev_close is not None:
        print(f"Previous close (reference for percentage change): {prev_close}")
    else:
        print("Failed to get previous close from local daily data; will use the first intraday open as reference.")

    now = datetime.now()

    # If market has already closed: fetch full-day minute data and save immediately
    if is_after_close(now):
        print("Market is already closed; fetching full intraday data and saving...")
        bars = fetch_recent_min_kline(STOCK_CODE, region=region, limit=5000)
        if not bars:
            print("API did not return any minute data")
            return

        df = convert_to_df(bars)
        if df.empty:
            print("No data after converting to DataFrame")
            return

        df_today = df[df["datetime"].dt.date == today]
        if df_today.empty:
            print(f"No minute data obtained for {today}")
            return

        os.makedirs("data", exist_ok=True)
        file_name = f"{STOCK_CODE}_{today.strftime('%Y%m%d')}_mins.csv"
        out_path = os.path.join("data", file_name)
        df_today.to_csv(out_path, index=False)
        print(f"Today's minute data saved: {out_path} ({len(df_today)} rows)")
        return

    # During trading hours: request once per whole minute, then save at close
    print("Currently in trading hours; will request once per whole minute until close (printing each bar)...")

    all_df = pd.DataFrame()
    seen_times: set[datetime] = set()
    # Reference price for percentage change: prefer previous close, fall back to first intraday open
    base_price: float | None = prev_close

    # 先等待到下一个整数分钟再开始循环
    sleep_until_next_minute()

    while True:
        now = datetime.now()
        if now.date() != today or is_after_close(now):
            break

        if not is_trading_time(now):
            # Non-trading periods (e.g. lunch break): just wait for the next minute
            sleep_until_next_minute()
            continue

        bars = fetch_recent_min_kline(STOCK_CODE, region=region, limit=1000)
        if not bars:
            sleep_until_next_minute()
            continue

        df = convert_to_df(bars)
        if df.empty:
            sleep_until_next_minute()
            continue

        df_today = df[df["datetime"].dt.date == today]
        if df_today.empty:
            sleep_until_next_minute()
            continue

        new_rows = df_today[~df_today["datetime"].isin(seen_times)].copy()

        if not new_rows.empty:
            # Update set of seen timestamps
            seen_times.update(new_rows["datetime"].tolist())

            # Accumulate all data for final save
            all_df = pd.concat([all_df, new_rows], ignore_index=True)
            all_df.drop_duplicates(subset=["datetime"], inplace=True)
            all_df.sort_values("datetime", inplace=True)

            # If reference price not yet initialized: use the first intraday open
            if base_price is None:
                first_row = all_df.iloc[0]
                base_price = float(first_row["open"]) if first_row["open"] != 0 else None

            # Print each newly added minute bar without extra headers and append percentage change
            for _, row in new_rows.iterrows():
                close_price = float(row["close"])
                if base_price and base_price != 0:
                    pct_change = (close_price / base_price - 1.0) * 100.0
                    pct_str = f"{pct_change:.2f}%"
                else:
                    pct_str = "N/A"

                print(
                    f"{row['datetime']} "
                    f"{row['open']} {row['high']} {row['low']} "
                    f"{row['close']} {row['volume']} {pct_str}"
                )

        # Wait until the next whole minute
        sleep_until_next_minute()

    if all_df.empty:
        print("No data accumulated during the entire trading session; no file will be saved.")
        return

    os.makedirs("data", exist_ok=True)
    file_name = f"{STOCK_CODE}_{today.strftime('%Y%m%d')}_mins.csv"
    out_path = os.path.join("data", file_name)
    all_df.to_csv(out_path, index=False)
    print(f"\nMarket closed; accumulated minute data saved: {out_path} ({len(all_df)} rows)")


if __name__ == "__main__":
    main()
