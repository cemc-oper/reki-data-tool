import numpy as np
import xarray as xr
import pandas as pd


def rmse(forecast, analysis):
    return np.sqrt(
        np.sum(
            np.power(forecast - analysis, 2)
            /
            np.product(analysis.shape)
        )
    )
