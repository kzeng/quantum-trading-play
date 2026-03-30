import os
import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt


def load_000988_data(data_dir: str = "./data") -> pd.DataFrame:
    """Load all CSV files for 000988 from 2020-2026 and merge into a single DataFrame."""
    frames = []
    for year in range(2020, 2026):
        file_name = f"000988_{year}.csv"
        path = os.path.join(data_dir, file_name)
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No 000988_2020-2026 data files were found")

    data = pd.concat(frames, ignore_index=True)
    data.sort_values("datetime", inplace=True)
    data.reset_index(drop=True, inplace=True)
    return data


# Read all 2020-2026 data from local files
df = load_000988_data()

# Define double moving average strategy
class DoubleMAStrategy(bt.Strategy):
    params = (
        ("short_period", 7),
        ("long_period", 14),
        ("print_log", True),
    )

    def __init__(self):
        self.short_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.short_period
        )
        self.long_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.long_period
        )
        self.crossover = bt.indicators.CrossOver(self.short_ma, self.long_ma)

    def next(self):
        if not self.position:
            if self.crossover[0] == 1:
                size = int(self.broker.getcash() * 0.95 / self.data.close[0])
                self.buy(size=size)
                if self.params.print_log:
                    print(f"{self.data.datetime.date()} 买入 {size} 股，价格 {self.data.close[0]:.2f}")
        else:
            if self.crossover[0] == -1:
                self.close()
                if self.params.print_log:
                    print(f"{self.data.datetime.date()} Sell at price {self.data.close[0]:.2f}")

# Create backtesting engine
cerebro = bt.Cerebro()

# Load data
data = bt.feeds.PandasData(
    dataname=df,
    datetime='datetime',
    open='open',
    high='high',
    low='low',
    close='close',
    volume='volume',
)
cerebro.adddata(data)

# Add strategy
cerebro.addstrategy(DoubleMAStrategy)

# Set initial cash and commission
cerebro.broker.setcash(100000.0)
cerebro.broker.setcommission(commission=0.001)

# Run backtest
print(f"Initial portfolio value: {cerebro.broker.getvalue():.2f}")
cerebro.run()
print(f"Final portfolio value: {cerebro.broker.getvalue():.2f}")

# Plot results
cerebro.plot()