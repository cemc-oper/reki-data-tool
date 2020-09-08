import typing

import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon

from .index import (
    mse,
    me,
    mae,
)


def calculate_point_surface(
        forecast_field: xr.DataArray,
        obs_table: pd.DataFrame,
        obs_key: str,
        domain: typing.List or typing.Tuple or None = None,
        mask_field: xr.DataArray or None = None,
) -> pd.DataFrame:
    if mask_field is not None:
        masked_forecast_field = forecast_field.where(
            mask_field == 1
        )
    else:
        masked_forecast_field = forecast_field

    geo_obs_table = gpd.GeoDataFrame(
        obs_table,
        geometry=gpd.points_from_xy(
            obs_table.longitude,
            obs_table.latitude
        )
    )

    if domain is not None:
        # domain = [20, 55, 70, 145]
        polygen = Polygon([
            (domain[2], domain[0]),
            (domain[2], domain[1]),
            (domain[3], domain[1]),
            (domain[3], domain[0]),
        ])

        domain_geo_obs_table = geo_obs_table[geo_obs_table.intersects(polygen)].copy()
    else:
        domain_geo_obs_table = geo_obs_table

    def get_nearest_value(line):
        value = masked_forecast_field.sel(
            longitude=line["longitude"],
            latitude=line["latitude"],
            method="nearest"
        ).item()
        return value

    forecast_key = "forecast"
    domain_geo_obs_table[forecast_key] = domain_geo_obs_table.apply(
        get_nearest_value,
        axis="columns",
    )

    table = domain_geo_obs_table.dropna()

    index_rmse = np.sqrt(mse(table, obs_key=obs_key, forecast_key=forecast_key))
    if index_rmse > 200:
        index_rmse = np.nan

    index_me = me(table, obs_key=obs_key, forecast_key=forecast_key)
    if index_me < -200:
        index_me = np.nan

    index_mae = mae(table, obs_key=obs_key, forecast_key=forecast_key)
    if index_mae > 200:
        index_mae = np.nan

    index_dict = {
        "rmse": [index_rmse],
        "me": [index_me],
        "mae": [index_mae],
    }

    df = pd.DataFrame(index_dict)

    return df