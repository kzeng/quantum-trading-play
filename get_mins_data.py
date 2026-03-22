

import requests
from datetime import datetime, timezone
import pandas as pd


API_TOKEN = "cdce1f21dccb454b9c8b130a17af35b3f8a6ae87b9e8477d8069e9aa0d72e31f"  # 替换为你的 token

# 股票代码列表
scocket_code_list = ["600519"]  # 贵州茅台

url = "https://api.itick.org/stock/kline"


def fetch_min_kline(code: str, limit: int = 1000, end_ts_ms: int = None):
    """从 itick 获取最近 limit 根 1 分钟 K 线数据，可指定结束时间戳 et（毫秒）。"""
    params = {
        "region": "SH",
        "code": code,
        "kType": 1,  # 1 = 1分钟K线
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
        print(f"{code} 请求失败: {data}")
        return []

    return data.get("data", [])


def fetch_min_kline_range(code: str, start_dt: datetime, end_dt: datetime, limit_per_request: int = 1000):
    """按时间区间获取分钟线：通过多次请求 et+limit 向前翻页直到覆盖 [start_dt, end_dt]。"""

    # 使用 UTC 计算时间戳（接口文档示例中的 t 注释为 UTC）
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

        # 如果这批数据已经覆盖到起点，或者数量小于 limit，说明没有更早数据了
        if min_t <= start_ts_ms or len(batch) < limit_per_request:
            break

        # 下一次往前翻页
        current_end = min_t - 1

    # 过滤到指定时间区间内，并按时间去重
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
    """把 itick 返回的数据转换为 DataFrame。"""
    rows = []
    for bar in bars:
        ts = bar["t"]
        # 判断是秒还是毫秒时间戳
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
        print(f"\n开始处理股票: {scocket_code}")

        # 先尝试获取 2025 年全年的分钟线
        start_2025 = datetime(2025, 1, 1, 0, 0, 0)
        end_2025 = datetime(2025, 12, 31, 23, 59, 59)
        print("正在拉取 2025 年分钟数据...")
        bars_2025 = fetch_min_kline_range(scocket_code, start_2025, end_2025, limit_per_request=1000)

        if not bars_2025:
            print(f"{scocket_code} 2025 年没有分钟数据（接口未返回该年份数据或权限受限）")
        else:
            df_2025 = convert_to_df(bars_2025)
            file_2025 = f"{scocket_code}_2025_mins.csv"
            df_2025.to_csv(file_2025, index=False)
            print(f"{scocket_code} 2025 年分钟数据已保存: {file_2025} （{len(df_2025)} 条）")

        # 再获取最近一段（实时）分钟数据，按实际年份拆分（延续你之前的用法）
        print("\n正在拉取最近一段分钟数据...")
        bars_recent = fetch_min_kline(scocket_code, limit=100000)
        df_recent = convert_to_df(bars_recent)

        if df_recent.empty:
            print(f"{scocket_code} 最近没有获取到分钟数据")
            continue

        available_years = sorted(df_recent["datetime"].dt.year.unique())
        print(f"{scocket_code} 最近数据实际包含年份: {available_years}")

        for year_int in available_years:
            df_year = df_recent[df_recent["datetime"].dt.year == year_int]
            year = str(year_int)

            file_name = f"{scocket_code}_{year}_mins.csv"
            df_year.to_csv(file_name, index=False)
            print(f"{scocket_code} {year} 年分钟数据已保存: {file_name} （{len(df_year)} 条）")


if __name__ == "__main__":
    main()