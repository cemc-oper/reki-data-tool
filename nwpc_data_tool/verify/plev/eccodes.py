"""
使用 nwpc_data.grib.eccodes 中的 message 系列 API 加载要素场为 eccodes GRIB message，并进行计算。

Notes
-----
本文件中的代码来自 GetPy 项目，并有部分修改。GetPy 项目是目前仍属于 NWPC 的内部项目
"""
import typing

import numpy as np
import pandas as pd

from .index import (
    mse,
    bias,
    absolute_bias,
    std,
    rmsem,
    rmsep,
    acc,
)


def calculate_plev_stats(
        forecast_array: np.ndarray,
        analysis_array: np.ndarray,
        climate_array: np.ndarray,
        domain: typing.List,
) -> pd.DataFrame:
    # 坐标网格
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
    domain_climate_array = climate_array[start_j:end_j, start_i:end_i]

    latitudes = llat[start_j:end_j, start_i:end_i]

    df = pd.DataFrame({
        "rmse": [np.sqrt(mse(domain_forecast_array, domain_analysis_array, latitudes))],
        "bias": [bias(domain_forecast_array, domain_analysis_array, latitudes)],
        "absolute_bias": [absolute_bias(domain_forecast_array, domain_analysis_array, latitudes)],
        "std": [std(domain_forecast_array, domain_analysis_array, latitudes)],
        "rmsem": [rmsem(domain_forecast_array, domain_analysis_array, latitudes)],
        "rmsep": [rmsep(domain_forecast_array, domain_analysis_array, latitudes)],
        "acc": [acc(domain_forecast_array, domain_analysis_array, domain_climate_array, latitudes)],
    })

    if (df["rmse"] > 1000.0).item():
        df = df[:] = -999

    return df



