import random

import pandas as pd
import numpy as np


# 起报时间
START_TIME = pd.to_datetime("2022-02-27 00:00:00")

# 预报时效
FORECAST_TIME = pd.Timedelta(hours=24)

# 输出目录
OUTPUT_DIRECTORY = "/g11/wangdp/project/work/data/playground/operation/gfs/ne/output"


def get_random_start_time(freq="D", date_offset=2, date_length=10) -> pd.Timestamp:
    end_date = pd.Timestamp.now().normalize() - pd.Timedelta(days=date_offset)
    start_date = end_date - pd.Timedelta(days=date_length)
    date_list = list(pd.date_range(start_date, end_date, freq=freq))
    random.shuffle(date_list)
    return date_list[0]


def get_random_forecast_time() -> pd.Timedelta:
    forecast_list = np.concatenate([
        np.arange(0, 121, 3),
        np.arange(126, 241, 6),
    ])
    np.random.shuffle(forecast_list)
    return pd.Timedelta(hours=forecast_list[0])
