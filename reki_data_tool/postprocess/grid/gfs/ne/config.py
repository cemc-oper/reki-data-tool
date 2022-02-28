import pandas as pd

# 起报时间
START_TIME = pd.to_datetime("2022-02-27 00:00:00")

# 预报时效
FORECAST_TIME = pd.Timedelta(hours=24)

# 输出目录
OUTPUT_DIRECTORY = "/g11/wangdp/project/work/data/playground/operation/gfs/ne/output"

