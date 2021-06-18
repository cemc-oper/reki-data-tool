import xarray as xr
import numpy as np
import pandas as pd
from tqdm.auto import tqdm


def load_all_dataset(
        data_path_root,
        parameter_list,
        show_progress=True
):
    dss = []
    if show_progress:
        ps = tqdm(parameter_list)
    else:
        ps = parameter_list
    for parameter in ps:
        name = parameter.get("name", parameter["parameter"])
        ds = xr.open_mfdataset(f"{data_path_root}/*.{name}.nc")
        dss.append(ds)

    ds = xr.merge(dss)
    return ds


def get_forecast_time_list(forecast_time):
    forecast_time_range = pd.to_timedelta(np.arange(0, 167, 6), unit="h").append(
        pd.to_timedelta(np.arange(168, 246, 12), unit="h")
    )

    if forecast_time > pd.Timedelta(hours=60):
        time_list = forecast_time_range[
            (forecast_time_range >= forecast_time - pd.Timedelta(hours=60))
            &
            (forecast_time_range <= forecast_time)
        ]
    else:
        time_list = forecast_time_range[
            (forecast_time_range <= forecast_time)
        ]
    return time_list


def reshape_array_to_samples(array: np.ndarray):
    s = array.shape
    return array.reshape(s[0], np.prod(s[1:]))


def reshape_array_to_sample(array: np.ndarray):
    s = array.shape
    return array.reshape(1, np.prod(s))