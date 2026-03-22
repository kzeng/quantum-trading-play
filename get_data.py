import baostock as bs
import pandas as pd


# define stock code list
scocket_code_list = ["600519"]  # 贵州茅台

# define date range list (start_date, end_date)
date_range_list = [
    ("2020-01-01", "2020-12-31"),
    ("2021-01-01", "2021-12-31"),
    ("2022-01-01", "2022-12-31"),
    ("2023-01-01", "2023-12-31"),
    ("2024-01-01", "2024-12-31"),
    ("2025-01-01", "2025-12-31"),

]

# 登录系统
lg = bs.login()
print("登录成功")

for scocket_code in scocket_code_list:
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
        data_list = []
        while (rs.error_code == "0") & rs.next():
            data_list.append(rs.get_row_data())

        if not data_list:
            print("没有获取到数据，请检查日期范围或股票代码")
            continue

        df = pd.DataFrame(data_list, columns=rs.fields)

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
        df.to_csv(file_name, index=False)
        print(f"\n数据已保存到 {file_name}")

# 登出系统
bs.logout()
print("登出成功")