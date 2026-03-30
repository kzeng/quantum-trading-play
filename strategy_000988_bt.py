import os
import pandas as pd
import backtrader as bt


class EMARsiStrategy(bt.Strategy):
    params = dict(
        fast_period=10,    # EMA fast line period
        slow_period=20,    # EMA slow line period
        rsi_period=60,     # RSI period
        rsi_buy=30,        # RSI threshold for buying
        rsi_sell=60,       # RSI threshold for selling
        stop_loss=0.30,    # 30% stop loss (sell when price drops 30%)
        take_profit=0.99,  # 99% take profit (sell when price rises 99%)
        print_log=True,    # whether to print trade logs
    )

    def __init__(self):
        # Indicators
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
            # Do nothing while there is a pending order
            return

        price = self.data.close[0]

        # No open position: look for entry signal
        if not self.position:
            # Condition: fast EMA crosses above slow EMA and RSI is strong
            if self.crossover[0] == 1 and self.rsi[0] > self.p.rsi_buy:
                cash = self.broker.getcash()
                size = int(cash * 0.95 / price)
                if size > 0:
                    self.log(f"买入 {size} 股，价格 {price:.2f} (RSI={self.rsi[0]:.2f})")
                    self.order = self.buy(size=size)
                    self.entry_price = price
        else:
            # Position is open: consider stop loss/take profit or exit signals
            if self.entry_price is None:
                self.entry_price = self.position.price

            # Stop loss / take profit
            if price <= self.entry_price * (1 - self.p.stop_loss):
                self.log(f"Stop loss triggered, sell at {price:.2f}")
                self.order = self.close()
                self.entry_price = None
                return

            if price >= self.entry_price * (1 + self.p.take_profit):
                self.log(f"Take profit triggered, sell at {price:.2f}")
                self.order = self.close()
                self.entry_price = None
                return

            # Technical exit: bearish crossover + RSI weakening
            if self.crossover[0] == -1 and self.rsi[0] < self.p.rsi_sell:
                self.log(f"Technical exit, sell at {price:.2f} (RSI={self.rsi[0]:.2f})")
                self.order = self.close()
                self.entry_price = None

    def notify_order(self, order):
        if order.status in [order.Completed, order.Canceled, order.Rejected]:
            self.order = None


def load_000988_data(data_dir: str = "./data") -> pd.DataFrame:
    """Load all CSV files for 000988 from 2020-2026 and merge into one DataFrame."""
    frames = []
    for year in range(2020, 2026):
        file_name = f"000988_{year}.csv"
        path = os.path.join(data_dir, file_name)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        # Ensure datetime column is parsed as datetime type
        df["datetime"] = pd.to_datetime(df["datetime"])
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No 000988_2020-2026 data files were found")

    data = pd.concat(frames, ignore_index=True)
    data.sort_values("datetime", inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data


def run_backtest():
    # Load data
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

    # Add strategy
    cerebro.addstrategy(EMARsiStrategy)

    # Initial capital and commission
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)

    print(f"Initial portfolio value: {cerebro.broker.getvalue():.2f}")
    cerebro.run()
    print(f"Final portfolio value: {cerebro.broker.getvalue():.2f}")

    # Plot results (requires a graphical environment)
    cerebro.plot()


if __name__ == "__main__":
    run_backtest()
