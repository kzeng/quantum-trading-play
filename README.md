

# Quantum Trading Play


## Create VENV  

创建名为 qt-venv 的环境，指定 Python 3.12

```bash
conda create -n qt-venv python=3.12 -y
conda activate qt-venv
```

## Install Packages

```bash

# 先通过 conda 安装一些基础包（可选，但推荐）
pip install pandas numpy matplotlib 

# 再用 pip 安装量化框架和数据源
# Doc: https://www.backtrader.com/docu/
# Github: https://github.com/mementum/backtrader
pip install backtrader

# 安装数据源（建议用 baostock，依赖简单）
pip install baostock

# quant1024（跨市场，支持分钟)
pip install quant1024

# iTick:  https://itick.io/dashboard
# Doc: https://itick.io/dashboard
# Toekn: cdce1f21dccb454b9c8b130a17af35b3f8a6ae87b9e8477d8069e9aa0d72e31f

```