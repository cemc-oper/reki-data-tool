"""
使用 nwpc_data.grib.eccodes 中的 field 系列 API 加载要素场为 xarray.DataArray 对象，并进行计算。
"""
import typing

import xarray as xr
import numpy as np
import pandas as pd


from .index import (
    mse,
    me,
    mae,
    sd,
    rmsem,
    rmsep,
    acc,
)


def calculate_plev_stats(
        forecast_field: xr.DataArray,
        analysis_field: xr.DataArray,
        climate_field: xr.DataArray,
        domain: typing.List,
) -> pd.DataFrame:
    climate_field["longitude"] = analysis_field.longitude

    # 提取子区域
    domain_forecast_field = forecast_field.sel(
        latitude=slice(*domain[1::-1]),
        longitude=slice(*domain[2:]),
    )

    domain_analysis_field = analysis_field.sel(
        latitude=slice(*domain[1::-1]),
        longitude=slice(*domain[2:]),
    )

    domain_climate_field = climate_field.sel(
        latitude=slice(*domain[1::-1]),
        longitude=slice(*domain[2:]),
    )

    latitudes = forecast_field.latitude * xr.ones_like(forecast_field)

    df = pd.DataFrame({
        "rmse": [np.sqrt(mse(domain_forecast_field, domain_analysis_field, latitudes)).values],
        "me": [me(domain_forecast_field, domain_analysis_field, latitudes).values],
        "mae": [mae(domain_forecast_field, domain_analysis_field, latitudes).values],
        "sd": [sd(domain_forecast_field, domain_analysis_field, latitudes).values],
        "rmsem": [rmsem(domain_forecast_field, domain_analysis_field, latitudes).values],
        "rmsep": [rmsep(domain_forecast_field, domain_analysis_field, latitudes).values],
        "acc": [acc(domain_forecast_field, domain_analysis_field, domain_climate_field, latitudes).values],
    })

    if (df["rmse"] > 1000.0).item():
        df = df[:] = -999

    return df
