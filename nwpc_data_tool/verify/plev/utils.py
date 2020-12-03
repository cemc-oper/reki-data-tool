import pathlib
import typing

import pandas as pd
import numpy as np
import eccodes

from nwpc_data_tool.verify.data import get_message
from nwpc_data_tool.verify.plev.eccodes import calculate_plev_stats


def get_values(
    file_path: pathlib.Path,
    parameter: str or typing.Dict,
    level: float or typing.Dict,
) -> np.ndarray:
    message = get_message(
        file_path,
        parameter=parameter,
        level=level,
    )
    array = eccodes.codes_get_double_array(message, "values").reshape([121, 240])
    eccodes.codes_release(message)
    return array


def get_stats(
    forecast_array: np.ndarray,
    analysis_array: np.ndarray,
    climate_array: np.ndarray,
    domain: typing.List or typing.Tuple or None,
    current_forecast_hour: int,
    parameter: str,
    level: float,
    region: str,
) -> pd.DataFrame:
    df = calculate_plev_stats(
        forecast_array=forecast_array,
        analysis_array=analysis_array,
        climate_array=climate_array,
        domain=domain,
    )
    # 增加元信息
    df["forecast_hour"] = current_forecast_hour
    df["variable"] = parameter
    df["level"] = level
    df["region"] = region
    return df


def get_total_df(df_list: typing.List[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(df_list, ignore_index=True)
    df = df.set_index(["forecast_hour", "variable", "level", "region"])
    return df


def save_df(
        df: pd.DataFrame,
        start_hour: pd.Timestamp,
        df_index: pd.DataFrame,
        output_path: typing.Optional[pathlib.Path],
):
    merged_df = pd.merge(
        df_index, df,
        left_index=True,
        right_index=True,
        how="left"
    ).fillna(-999.0)
    plain_merged_df = merged_df.reset_index()
    if output_path is None:
        output_path = f"stats.{start_hour.strftime('%Y%m%d%H')}.csv",
    plain_merged_df.to_csv(
        output_path,
        index=False,
        float_format="%.6f",
    )
