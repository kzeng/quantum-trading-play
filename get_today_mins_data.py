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


def is_trading_time(now: datetime) -> bool:
    """判断是否在交易时间段内（不含集合竞价，只按连续竞价时段）。"""

    t = now.time()
    morning_start = dtime(9, 30)
    morning_end = dtime(11, 30)
    afternoon_start = dtime(13, 0)
    afternoon_end = dtime(15, 0)

    in_morning = morning_start <= t <= morning_end
    in_afternoon = afternoon_start <= t <= afternoon_end
    return in_morning or in_afternoon


def is_after_close(now: datetime) -> bool:
    """是否已经收盘（15:00 之后）。"""

    return now.time() >= dtime(15, 0)


def sleep_until_next_minute() -> None:
    """睡眠到下一个整数分钟。"""

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
    print(f"开始处理 {region}.{STOCK_CODE} {today} 的每分钟数据...")

    now = datetime.now()

    # 如果已经收盘：直接获取当日全部分钟数据并保存
    if is_after_close(now):
        print("已经过收盘时间，直接获取当日全部数据并保存...")
        bars = fetch_recent_min_kline(STOCK_CODE, region=region, limit=5000)
        if not bars:
            print("API 没有返回分钟数据")
            return

        df = convert_to_df(bars)
        if df.empty:
            print("转换为 DataFrame 后无数据")
            return

        df_today = df[df["datetime"].dt.date == today]
        if df_today.empty:
            print(f"未获取到 {today} 的分钟数据")
            return

        os.makedirs("data", exist_ok=True)
        file_name = f"{STOCK_CODE}_{today.strftime('%Y%m%d')}_mins.csv"
        out_path = os.path.join("data", file_name)
        df_today.to_csv(out_path, index=False)
        print(f"今日分钟数据已保存: {out_path} （{len(df_today)} 条）")
        return

    # 交易时间内：每个整数分钟请求一次，安静运行，收盘后统一保存
    print("当前在交易时间内，将每个整数分钟请求一次数据，直到收盘（打印每笔数据）...")

    all_df = pd.DataFrame()
    seen_times: set[datetime] = set()
    base_price: float | None = None  # 用于计算涨跌幅的基准价（当天第一笔的开盘价）

    # 先等待到下一个整数分钟再开始循环
    sleep_until_next_minute()

    while True:
        now = datetime.now()
        if now.date() != today or is_after_close(now):
            break

        if not is_trading_time(now):
            # 午休等非交易时间，直接等待下一分钟
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
            # 更新已见时间
            seen_times.update(new_rows["datetime"].tolist())

            # 累积所有数据用于最后保存
            all_df = pd.concat([all_df, new_rows], ignore_index=True)
            all_df.drop_duplicates(subset=["datetime"], inplace=True)
            all_df.sort_values("datetime", inplace=True)

            # 初始化基准价：当天第一笔分钟K线的开盘价
            if base_price is None:
                first_row = all_df.iloc[0]
                base_price = float(first_row["open"]) if first_row["open"] != 0 else None

            # 逐行打印本次新增的分钟数据（不带额外标题和列名），并在末尾追加涨跌幅%
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

        # 等待到下一个整数分钟
        sleep_until_next_minute()

    if all_df.empty:
        print("整个交易时段内未累计到任何数据，不保存文件。")
        return

    os.makedirs("data", exist_ok=True)
    file_name = f"{STOCK_CODE}_{today.strftime('%Y%m%d')}_mins.csv"
    out_path = os.path.join("data", file_name)
    all_df.to_csv(out_path, index=False)
    print(f"\n收盘，累计分钟数据已保存: {out_path} （{len(all_df)} 条）")


if __name__ == "__main__":
    main()
