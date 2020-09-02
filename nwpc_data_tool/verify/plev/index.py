import numpy as np
import xarray as xr


def mse(
        forecast_field: np.ndarray or xr.DataArray,
        analysis_field: np.ndarray or xr.DataArray,
        latitudes: np.ndarray or xr.DataArray
) -> np.ndarray or xr.DataArray:
    """
    Mean Square Error (MSE)

    Parameters
    ----------
    forecast_field
    analysis_field
    latitudes

    Returns
    -------

    """
    result = np.sum(
        np.power(forecast_field - analysis_field, 2) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(np.cos(latitudes * np.pi / 180.0))
    return result


def me(
        forecast_field: np.ndarray or xr.DataArray,
        analysis_field: np.ndarray or xr.DataArray,
        latitudes: np.ndarray or xr.DataArray
) -> np.ndarray or xr.DataArray:
    """
    Mean Error (ME), also called Bias.

    Parameters
    ----------
    forecast_field: np.ndarray or xr.DataArray
    analysis_field: np.ndarray or xr.DataArray
    latitudes: np.ndarray or xr.DataArray

    Returns
    -------
    np.ndarray or xr.DataArray

    """
    result = np.sum(
        (forecast_field - analysis_field) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(np.cos(latitudes * np.pi / 180.0))
    return result


def mae(
        forecast_field: np.ndarray or xr.DataArray,
        analysis_field: np.ndarray or xr.DataArray,
        latitudes: np.ndarray or xr.DataArray
) -> np.ndarray or xr.DataArray:
    result = np.sum(
        np.abs(forecast_field - analysis_field) * np.cos(latitudes * np.pi / 180.0)
    ) / np.sum(np.cos(latitudes * np.pi / 180.0))
    return result


def sd(
        forecast_field: np.ndarray or xr.DataArray,
        analysis_field: np.ndarray or xr.DataArray,
        latitudes: np.ndarray or xr.DataArray
) -> np.ndarray or xr.DataArray:
    bias_result = me(forecast_field, analysis_field, latitudes)
    result = np.sqrt(
        np.sum(
            np.power(forecast_field - analysis_field - bias_result, 2) * np.cos(latitudes * np.pi / 180.)
        ) / np.sum(np.cos(latitudes * np.pi / 180.))
    )
    return result


def rmsem(
        forecast_field: np.ndarray or xr.DataArray,
        analysis_field: np.ndarray or xr.DataArray,
        latitudes: np.ndarray or xr.DataArray
) -> np.ndarray or xr.DataArray:
    return np.abs(me(forecast_field, analysis_field, latitudes))


def rmsep(
        forecast_field: np.ndarray or xr.DataArray,
        analysis_field: np.ndarray or xr.DataArray,
        latitudes: np.ndarray or xr.DataArray
) -> np.ndarray or xr.DataArray:
    return np.sqrt(mse(forecast_field, analysis_field, latitudes)) - rmsem(forecast_field, analysis_field, latitudes)


def acc(
        forecast_field: np.ndarray or xr.DataArray,
        analysis_field: np.ndarray or xr.DataArray,
        climate_field: np.ndarray or xr.DataArray,
        latitudes: np.ndarray or xr.DataArray
) -> np.ndarray or xr.DataArray:
    """
    Anomaly correlation coefficient (ACC)

    Parameters
    ----------
    forecast_field
    analysis_field
    climate_field
    latitudes

    Returns
    -------

    """
    forecast_climate = me(forecast_field, climate_field, latitudes)
    obs_climate = me(analysis_field, climate_field, latitudes)
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
