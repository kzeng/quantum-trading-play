import pandas as pd
import backtrader as bt
import matplotlib.pyplot as plt



# 从本地读取数据
df = pd.read_csv("./data/000988_2020.csv")
df['datetime'] = pd.to_datetime(df['datetime'])

# 定义双均线策略
class DoubleMAStrategy(bt.Strategy):
    params = (
        ("short_period", 3),
        ("long_period", 10),
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
                    print(f"{self.data.datetime.date()} 卖出，价格 {self.data.close[0]:.2f}")

# 创建回测引擎
cerebro = bt.Cerebro()

# 加载数据
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

# 添加策略
cerebro.addstrategy(DoubleMAStrategy)

# 设置初始资金和手续费
cerebro.broker.setcash(100000.0)
cerebro.broker.setcommission(commission=0.001)

# 运行回测
print(f"初始资金: {cerebro.broker.getvalue():.2f}")
cerebro.run()
print(f"最终资金: {cerebro.broker.getvalue():.2f}")

# 绘图
cerebro.plot()