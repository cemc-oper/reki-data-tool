"""
使用 reki.grib.eccodes 中的 message 系列 API 加载要素场为 eccodes GRIB message，并进行计算。

Notes
-----
本文件中的代码来自 GetPy 项目，并有部分修改。GetPy 项目是目前仍属于 NWPC 的内部项目
"""
import typing

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
        forecast_array: np.ndarray,
        analysis_array: np.ndarray,
        climate_array: np.ndarray or None = None,
        domain: typing.List or typing.Tuple or None = None,
) -> pd.DataFrame:
    """

    Parameters
    ----------
    forecast_array
    analysis_array
    climate_array
    domain: typing.List
        区域范围，[south_lat, north_lat, west_lon, east_lon]
        例如 [20, 90, 0, 360] 表示北半球（NHEM）

    Returns
    -------

    """
    # 坐标网格
    if domain is None:
        domain = (-90, 90, 0, 360)
    lat = np.arange(90, -90 - 1.5, -1.5)
    lon = np.arange(0, 360, 1.5)
    llon, llat = np.meshgrid(lon, lat)

    # 计算边界点的序号
    start_j = int((90.0 - domain[1]) / 1.5 + 1)
    end_j = int((90.0 - domain[0]) / 1.5 + 1)
    start_i = int(domain[2] / 1.5)
    end_i = int(domain[3] / 1.5)

    # 提取子区域
    domain_forecast_array = forecast_array[start_j:end_j, start_i:end_i]
    domain_analysis_array = analysis_array[start_j:end_j, start_i:end_i]
    if climate_array is not None:
        domain_climate_array = climate_array[start_j:end_j, start_i:end_i]
    else:
        domain_climate_array = None

    latitudes = llat[start_j:end_j, start_i:end_i]

    index_rmse = np.sqrt(mse(domain_forecast_array, domain_analysis_array, latitudes))
    index_me = me(domain_forecast_array, domain_analysis_array, latitudes)
    index_mae = mae(domain_forecast_array, domain_analysis_array, latitudes)
    index_sd = sd(domain_forecast_array, domain_analysis_array, latitudes)
    index_rmsem = rmsem(domain_forecast_array, domain_analysis_array, latitudes)
    index_rmsep = rmsep(domain_forecast_array, domain_analysis_array, latitudes)

    index_dict = {
        "rmse": [index_rmse],
        "me": [index_me],
        "mae": [index_mae],
        "sd": [index_sd],
        "rmsem": [index_rmsem],
        "rmsep": [index_rmsep],
    }

    if domain_climate_array is not None:
        index_acc = acc(domain_forecast_array, domain_analysis_array, domain_climate_array, latitudes)
        index_dict["acc"] = [index_acc]

    df = pd.DataFrame(index_dict)

    if (df["rmse"] > 1000.0).item():
        df = df[:] = -999

    return df
