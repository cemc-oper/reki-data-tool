import numpy as np
import pandas as pd


def mse(
        table: pd.DataFrame,
        obs_key: str,
        forecast_key: str = "forecast",
) -> float:
    return np.power(table[forecast_key] - table[obs_key], 2).mean()


def me(
        table: pd.DataFrame,
        obs_key: str,
        forecast_key: str = "forecast",
) -> float:
    return (table[forecast_key] - table[obs_key]).mean()


def mae(
        table: pd.DataFrame,
        obs_key: str,
        forecast_key: str = "forecast",
) -> float:
    return (table[forecast_key] - table[obs_key]).abs().mean()
