import os
import pandas as pd
import backtrader as bt


class EMARsiStrategy(bt.Strategy):
    params = dict(
        fast_period=10,    # EMA 快线周期
        slow_period=20,    # EMA 慢线周期
        rsi_period=60,     # RSI 指标周期
        rsi_buy=30,        # 买入 RSI 阈值
        rsi_sell=60,       # 卖出 RSI 阈值
        stop_loss=0.30,    # 30% 止损（跌幅达到 30% 卖出）
        take_profit=0.99,  # 99% 止盈（涨幅达到 99% 卖出）
        print_log=True,    # 是否打印交易日志
    )

    def __init__(self):
        # 指标
        self.fast_ema = bt.ind.EMA(self.data.close, period=self.p.fast_period)
        self.slow_ema = bt.ind.EMA(self.data.close, period=self.p.slow_period)
        self.rsi = bt.ind.RSI(self.data.close, period=self.p.rsi_period)
        self.crossover = bt.ind.CrossOver(self.fast_ema, self.slow_ema)

        self.order = None
        self.entry_price = None

    def log(self, txt):
        if self.p.print_log:
            dt = self.data.datetime.datetime(0)
            print(f"{dt.strftime('%Y-%m-%d %H:%M:%S')} {txt}")

    def next(self):
        if self.order:
            # 有挂单时先不处理
            return

        price = self.data.close[0]

        # 无持仓，寻找入场机会
        if not self.position:
            # 条件：快线向上金叉慢线，且 RSI 强势
            if self.crossover[0] == 1 and self.rsi[0] > self.p.rsi_buy:
                cash = self.broker.getcash()
                size = int(cash * 0.95 / price)
                if size > 0:
                    self.log(f"买入 {size} 股，价格 {price:.2f} (RSI={self.rsi[0]:.2f})")
                    self.order = self.buy(size=size)
                    self.entry_price = price
        else:
            # 有持仓，考虑止盈/止损或反向信号
            if self.entry_price is None:
                self.entry_price = self.position.price

            # 止损 / 止盈
            if price <= self.entry_price * (1 - self.p.stop_loss):
                self.log(f"触发止损，卖出，价格 {price:.2f}")
                self.order = self.close()
                self.entry_price = None
                return

            if price >= self.entry_price * (1 + self.p.take_profit):
                self.log(f"触发止盈，卖出，价格 {price:.2f}")
                self.order = self.close()
                self.entry_price = None
                return

            # 技术性离场：死叉 + RSI 转弱
            if self.crossover[0] == -1 and self.rsi[0] < self.p.rsi_sell:
                self.log(f"技术离场，卖出，价格 {price:.2f} (RSI={self.rsi[0]:.2f})")
                self.order = self.close()
                self.entry_price = None

    def notify_order(self, order):
        if order.status in [order.Completed, order.Canceled, order.Rejected]:
            self.order = None


def load_000988_data(data_dir: str = "./data") -> pd.DataFrame:
    """加载 000988 在 2020-2026 年的所有 CSV，并合并为一个 DataFrame。"""
    frames = []
    for year in range(2020, 2026):
        file_name = f"000988_{year}.csv"
        path = os.path.join(data_dir, file_name)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        # 确保 datetime 为时间类型
        df["datetime"] = pd.to_datetime(df["datetime"])
        frames.append(df)

    if not frames:
        raise FileNotFoundError("未找到任何 000988_2020-2026 的数据文件")

    data = pd.concat(frames, ignore_index=True)
    data.sort_values("datetime", inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data


def run_backtest():
    # 加载数据
    df = load_000988_data()

    datafeed = bt.feeds.PandasData(
        dataname=df,
        datetime="datetime",
        open="open",
        high="high",
        low="low",
        close="close",
        volume="volume",
    )

    cerebro = bt.Cerebro()
    cerebro.adddata(datafeed)

    # 添加策略
    cerebro.addstrategy(EMARsiStrategy)

    # 初始资金与手续费
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    print(f"初始资金: {cerebro.broker.getvalue():.2f}")
    cerebro.run()
    print(f"最终资金: {cerebro.broker.getvalue():.2f}")

    # 画图（需要图形环境）
    cerebro.plot()


if __name__ == "__main__":
    run_backtest()
