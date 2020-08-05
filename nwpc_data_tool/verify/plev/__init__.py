import xarray as xr
import numpy as np
import pandas as pd

from nwpc_data.grib.eccodes import load_field_from_file


def calculate_plev_stats(
        parameter,
        level,
        domain,
        forecast_data_path=None,
        forecast_field=None,
        analysis_data_path=None,
        analysis_field=None,
        climate_data_path=None,
        climate_field=None,
):
    if forecast_field is None:
        if forecast_data_path is None:
            raise ValueError("forecast_field or forecast_data_path must be set")
        forecast_field = load_field_from_file(
            file_path=forecast_data_path,
            parameter=parameter,
            level_type="isobaricInhPa",
            level=level
        )

    if analysis_field is None:
        if analysis_data_path is None:
            raise ValueError("analysis_field or analysis_data_path must be set")
        analysis_field = load_field_from_file(
            file_path=analysis_data_path,
            parameter=parameter,
            level_type="isobaricInhPa",
            level=level,
        )

    if climate_field is None:
        if climate_data_path is None:
            raise ValueError("climate_field or climate_data_path must be set")
        climate_field = load_field_from_file(
            file_path=climate_data_path,
            parameter=parameter,
            level_type="isobaricInhPa",
            level=level,
        )
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

    return pd.DataFrame({
        "rmse": [np.sqrt(mse(domain_forecast_field, domain_analysis_field)).values],
        "bias": [bias(domain_forecast_field, domain_analysis_field).values],
        "absolute_bias": [absolute_bias(domain_forecast_field, domain_analysis_field).values],
        "std": [std(domain_forecast_field, domain_analysis_field).values],
        "rmsem": [rmsem(domain_forecast_field, domain_analysis_field).values],
        "rmsep": [rmsep(domain_forecast_field, domain_analysis_field).values],
        "acc": [acc(domain_forecast_field, domain_analysis_field, domain_climate_field).values],
    })


def mse(forecast_field, analysis_field):
    latitudes = forecast_field.latitude * xr.ones_like(forecast_field)
    result = np.sum(
        np.power(forecast_field - analysis_field, 2) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(
        np.cos(latitudes * np.pi / 180.0)
    )
    return result


def bias(forecast_field, analysis_field):
    latitudes = forecast_field.latitude * xr.ones_like(forecast_field)
    result = np.sum(
        (forecast_field - analysis_field) * np.cos(latitudes * np.pi / 180.)
    ) / np.sum(np.cos(latitudes * np.pi / 180.))
    return result


def absolute_bias(forecast_field, analysis_field):
    latitudes = forecast_field.latitude * xr.ones_like(forecast_field)
    result = np.sum(
        np.abs(forecast_field - analysis_field) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(np.cos(latitudes * np.pi / 180.0))
    return result


def std(forecast_field, analysis_field):
    latitudes = forecast_field.latitude * xr.ones_like(forecast_field)
    bias_result = bias(forecast_field, analysis_field)
    result = np.sqrt(
        np.sum(
            np.power(forecast_field - analysis_field - bias_result, 2) * np.cos(latitudes * np.pi / 180.)
        ) / np.sum(np.cos(latitudes * np.pi / 180.))
    )
    return result


def rmsem(forecast_field, analysis_field):
    return np.abs(bias(forecast_field, analysis_field))


def rmsep(forecast_field, analysis_field):
    return np.sqrt(mse(forecast_field, analysis_field)) - rmsem(forecast_field, analysis_field)


def acc(forecast_field, analysis_field, climate_field):
    latitudes = forecast_field.latitude * xr.ones_like(forecast_field)
    forecast_climate = bias(forecast_field, climate_field)
    obs_climate = bias(analysis_field, climate_field)
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
