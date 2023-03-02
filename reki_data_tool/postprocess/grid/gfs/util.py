import random
from typing import Optional

import numpy as np
import pandas as pd


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


def parse_time_options(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None
) -> (pd.Timestamp, pd.Timedelta):
    if start_time is None:
        start_time = get_random_start_time()
    else:
        start_time = pd.to_datetime(start_time, format="%Y%m%d%H")

    if forecast_time is None:
        forecast_time = get_random_forecast_time()
    else:
        forecast_time = pd.to_timedelta(forecast_time)
    return start_time, forecast_time
