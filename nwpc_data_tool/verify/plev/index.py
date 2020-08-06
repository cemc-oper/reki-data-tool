import numpy as np


def mse(forecast_field, analysis_field, latitudes):
    result = np.sum(
        np.power(forecast_field - analysis_field, 2) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(np.cos(latitudes * np.pi / 180.0))
    return result


def bias(forecast_field, analysis_field, latitudes):
    result = np.sum(
        (forecast_field - analysis_field) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(np.cos(latitudes * np.pi / 180.0))
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
