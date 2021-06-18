import pandas as pd
import xarray as xr
import numpy as np
from typing import List, Union


def get_train_periods(start_time: pd.Timestamp, forecast_time: pd.Timedelta):
    train_range_current_year = pd.Interval(
        start_time - pd.to_timedelta("35d"),
        start_time - forecast_time,
        closed="left"
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


def extract_test_output(ds, start_time, forecast_time):
    return ds.sel(
        time=start_time + forecast_time,
        step=pd.to_timedelta("0s")
    )


def extract_test_input(ds, start_time, forecast_time):
    return ds.sel(
        time=start_time,
        step=forecast_time
    )


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


def extract_train_input_output(
        input_ds: Union[xr.DataArray, xr.Dataset],
        output_ds: Union[xr.DataArray, xr.Dataset],
        train_periods,
        forecast_time: pd.Timedelta
):
    train_output_ds = extract_train_output(output_ds, train_periods, forecast_time)
    output_time = train_output_ds.time.values

    input_dss = [
        input_ds.sel(time=t-forecast_time, step=forecast_time)
        for t in output_time
        if pd.to_datetime(t-forecast_time) in input_ds.time
    ]
    train_input_ds = xr.concat(input_dss, dim="time")

    # 按输入集筛选目标数据
    train_output_ds = train_output_ds.sel(time=train_input_ds.time + forecast_time)
    return train_input_ds, train_output_ds
