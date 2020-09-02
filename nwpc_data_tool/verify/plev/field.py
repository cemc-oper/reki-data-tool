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
        climate_field: xr.DataArray or None = None,
        domain: typing.List or typing.Tuple or None = None,
) -> pd.DataFrame:

    # 提取子区域
    domain_forecast_field = forecast_field
    if domain is not None:
        domain_forecast_field = forecast_field.sel(
            latitude=slice(*domain[1::-1]),
            longitude=slice(*domain[2:]),
        )

    domain_analysis_field = analysis_field
    if domain is not None:
        domain_analysis_field = analysis_field.sel(
            latitude=slice(*domain[1::-1]),
            longitude=slice(*domain[2:]),
        )

    domain_climate_field = climate_field
    if climate_field is not None and domain is not None:
        climate_field["longitude"] = analysis_field.longitude
        domain_climate_field = climate_field.sel(
            latitude=slice(*domain[1::-1]),
            longitude=slice(*domain[2:]),
        )

    latitudes = domain_forecast_field.latitude * xr.ones_like(domain_forecast_field)

    index_rmse = np.sqrt(mse(domain_forecast_field, domain_analysis_field, latitudes)).values
    index_me = me(domain_forecast_field, domain_analysis_field, latitudes).values
    index_mae = mae(domain_forecast_field, domain_analysis_field, latitudes).values
    index_sd = sd(domain_forecast_field, domain_analysis_field, latitudes).values
    index_rmsem = rmsem(domain_forecast_field, domain_analysis_field, latitudes).values
    index_rmsep = rmsep(domain_forecast_field, domain_analysis_field, latitudes).values

    index_dict = {
        "rmse": [index_rmse],
        "me": [index_me],
        "mae": [index_mae],
        "sd": [index_sd],
        "rmsem": [index_rmsem],
        "rmsep": [index_rmsep],
    }

    if domain_climate_field is not None:
        index_acc = acc(domain_forecast_field, domain_analysis_field, domain_climate_field, latitudes).values
        index_dict["acc"] = [index_acc]

    df = pd.DataFrame(index_dict)

    if (df["rmse"] > 1000.0).item():
        df = df[:] = -999

    return df
