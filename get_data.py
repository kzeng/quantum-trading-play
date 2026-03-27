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
    """从 itick 获取最近 limit 根日 K 线数据，可指定结束时间戳 et（毫秒）。kType=8 表示日线。"""

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
        print("[itick] 日K 请求超时，本次跳过")
        return []
    except requests.exceptions.RequestException as exc:
        print(f"[itick] 日K 请求异常: {exc}")
        return []

    data = resp.json()
    if data.get("code") != 0:
        print(f"[itick] 日K 返回错误: {data}")
        return []

    return data.get("data", []) or []


def fetch_daily_kline_range_itick(code: str, region: str, start_dt: datetime, end_dt: datetime, limit_per_request: int = 1000) -> pd.DataFrame:
    """通过多次日 K 请求，获取 [start_dt, end_dt] 区间内的所有日线，并返回与 000988_2026.csv 一致格式的 DataFrame。"""

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

    # 过滤到区间内
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
                "date": dt,  # 先用 date，后面与 Baostock 流程统一
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

# 登录系统
lg = bs.login()
print("登录成功")

for scocket_code in scocket_code_list:
    if scocket_code.startswith("6"):
        full_code = f"sh.{scocket_code}"
    elif scocket_code.startswith("0") or scocket_code.startswith("3"):
        full_code = f"sz.{scocket_code}"
    else:
        full_code = f"sh.{scocket_code}"  # 目前假设为上交所股票

    for start_date, end_date in date_range_list:
        print(f"\n开始获取 {full_code} {start_date} 到 {end_date} 的日线数据...")

        rs = bs.query_history_k_data_plus(
            full_code,
            "date,open,high,low,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",  # 2表示前复权
        )

        # 打印结果
        print("请求数据返回码:", rs.error_code)
        print("请求数据返回信息:", rs.error_msg)

        # 转换为DataFrame
        data_list: list[list[str]] = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if data_list:
            # 使用 Baostock 返回的数据
            df = pd.DataFrame(data_list, columns=rs.fields)
        else:
            # Baostock 没有返回数据：尝试用 itick 日K 直接生成日线
            print("Baostock 未返回数据，尝试通过 itick 日K 生成日线...")

            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

            # itick 使用不带交易所前缀的代码 + region
            region = "SH" if scocket_code.startswith("6") else "SZ"
            df = fetch_daily_kline_range_itick(scocket_code, region=region, start_dt=start_dt, end_dt=end_dt)

            if df.empty:
                print("itick 日K 也未能获取到该区间的数据，跳过。")
                continue

        # 转换数据类型
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["date"] = pd.to_datetime(df["date"])

        # 重命名列名以匹配backtrader
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

        # 按日期排序
        df.sort_values("datetime", inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 查看数据
        print("\n数据预览:")
        print(df.head())
        print(f"\n总共获取 {len(df)} 条数据")
        print(f"日期范围: {df['datetime'].min()} 到 {df['datetime'].max()}")

        # 根据起始日期中的年份生成文件名
        year = start_date[:4]
        file_name = f"{scocket_code}_{year}.csv"

        # 保存到本地
        df.to_csv("./data/"+file_name, index=False)
        print(f"\n数据已保存到 {file_name}")

# 登出系统
bs.logout()
print("登出成功")