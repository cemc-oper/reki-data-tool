import pandas as pd
import xarray as xr
import numpy as np
from typing import List, Union, Tuple


def get_train_periods(start_time):
    train_range_current_year = pd.Interval(
        start_time - pd.to_timedelta("35d"),
        start_time,
        closed="both"
    )

    last_year_start_time = start_time - pd.DateOffset(years=1)
    train_range_last_year = pd.Interval(
        last_year_start_time - pd.to_timedelta("35d"),
        last_year_start_time + pd.to_timedelta("35d"),
        closed="both"
    )

    return [
        train_range_last_year,
        train_range_current_year
    ]


def extract_test_dataset(
        input_ds: xr.Dataset,
        output_ds: xr.Dataset,
        start_time,
        forecast_time
):
    total_hours = forecast_time / pd.Timedelta(hours=1)
    forecast_time_range = pd.to_timedelta([total_hours - t for t in range(66, -6, -6)], unit="h")
    test_input_ds = input_ds.sel(
        time=start_time,
        step=forecast_time_range
    )
    test_output_ds = output_ds.sel(
        time=start_time + forecast_time,
        step=pd.to_timedelta("0s")
    )
    return test_input_ds, test_output_ds


def extract_train_output(ds, train_periods, forecast_time):
    def get_period_output(period):
        p = period + forecast_time
        return ds.sel(
            time=slice(p.left, p.right),
            step=pd.to_timedelta("0s")
        )

    train_output_ds = xr.concat(
        [
            get_period_output(period) for period in train_periods
        ],
        dim="time"
    )
    return train_output_ds


def extract_train_dataset(
        input_ds: Union[xr.DataArray, xr.Dataset],
        output_ds: Union[xr.DataArray, xr.Dataset],
        train_periods,
        forecast_time: pd.Timedelta,
) -> Tuple[xr.Dataset, xr.Dataset]:
    train_output_ds = extract_train_output(output_ds, train_periods, forecast_time)
    output_time = train_output_ds.time.values

    total_hours = forecast_time / pd.Timedelta(hours=1)
    forecast_time_range = pd.to_timedelta([total_hours - t for t in range(66, -6, -6)], unit="h")

    input_dss = [
        input_ds.sel(time=t-forecast_time, step=forecast_time_range)
        for t in output_time
        if pd.to_datetime(t-forecast_time) in input_ds.time
    ]
    train_input_ds = xr.concat(input_dss, dim="time")

    # 按输入集筛选目标数据
    train_output_ds = train_output_ds.sel(time=train_input_ds.time + forecast_time)
    return train_input_ds, train_output_ds


def reshape_array_to_samples(array: np.ndarray):
    s = array.shape
    return array.reshape(s[0], np.prod(s[1:]))


def reshape_array_to_sample(array: np.ndarray):
    s = array.shape
    return array.reshape(1, np.prod(s))
