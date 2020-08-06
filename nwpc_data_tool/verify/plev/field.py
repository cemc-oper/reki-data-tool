"""
使用 nwpc_data.grib.eccodes 中的 field 系列 API 加载要素场为 xarray.DataArray 对象，并进行计算。
"""

import xarray as xr
import numpy as np
import pandas as pd


def calculate_plev_stats(
        forecast_field,
        analysis_field,
        climate_field,
        domain,
):
    climate_field["longitude"] = analysis_field.longitude

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
        "bias": [bias(domain_forecast_field, domain_analysis_field, latitudes).values],
        "absolute_bias": [absolute_bias(domain_forecast_field, domain_analysis_field, latitudes).values],
        "std": [std(domain_forecast_field, domain_analysis_field, latitudes).values],
        "rmsem": [rmsem(domain_forecast_field, domain_analysis_field, latitudes).values],
        "rmsep": [rmsep(domain_forecast_field, domain_analysis_field, latitudes).values],
        "acc": [acc(domain_forecast_field, domain_analysis_field, domain_climate_field, latitudes).values],
    })

    if (df["rmse"] > 1000.0).item():
        df = df[:] = -999

    return df


def mse(forecast_field, analysis_field, latitudes):
    result = np.sum(
        np.power(forecast_field - analysis_field, 2) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(
        np.cos(latitudes * np.pi / 180.0)
    )
    return result


def bias(forecast_field, analysis_field, latitudes):
    result = np.sum(
        (forecast_field - analysis_field) * np.cos(latitudes * np.pi / 180.)
    ) / np.sum(np.cos(latitudes * np.pi / 180.))
    return result


def absolute_bias(forecast_field, analysis_field, latitudes):
    result = np.sum(
        np.abs(forecast_field - analysis_field) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(np.cos(latitudes * np.pi / 180.0))
    return result


def std(forecast_field, analysis_field, latitudes):
    bias_result = bias(forecast_field, analysis_field, latitudes)
    result = np.sqrt(
        np.sum(
            np.power(forecast_field - analysis_field - bias_result, 2) * np.cos(latitudes * np.pi / 180.)
        ) / np.sum(np.cos(latitudes * np.pi / 180.))
    )
    return result


def rmsem(forecast_field, analysis_field, latitudes):
    return np.abs(bias(forecast_field, analysis_field, latitudes))


def rmsep(forecast_field, analysis_field, latitudes):
    return np.sqrt(mse(forecast_field, analysis_field, latitudes)) - rmsem(forecast_field, analysis_field, latitudes)


def acc(forecast_field, analysis_field, climate_field, latitudes):
    forecast_climate = bias(forecast_field, climate_field, latitudes)
    obs_climate = bias(analysis_field, climate_field, latitudes)
    acc1 = np.sum(
        (forecast_field - climate_field - forecast_climate) * (analysis_field - climate_field - obs_climate) * np.cos(latitudes * np.pi / 180.)
    )
    acc2 = np.sum(
        np.power(forecast_field - climate_field - forecast_climate, 2) * np.cos(latitudes * np.pi / 180.)
    )
    acc3 = np.sum(
        np.power(analysis_field - climate_field - obs_climate, 2) * np.cos(latitudes * np.pi / 180.)
    )
    result = acc1 / np.sqrt(acc2 * acc3)
    return result
