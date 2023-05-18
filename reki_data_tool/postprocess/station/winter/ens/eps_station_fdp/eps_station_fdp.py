from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd
from loguru import logger
from tqdm.auto import tqdm

from reki.data_finder import find_local_file
from reki.format.grib import load_field_from_file
from reki.operator import extract_point


PARAMETER_LIST = [
    ("u10", {"parameter": "10u"}),
    ("v10", {"parameter": "10v"}),
    ("gust10", {"parameter": "GUST"}),
    ("t2", {"parameter": "2t"}),
    ("rh2", {"parameter": "2r"}),
    ("vis", {"parameter": "VIS"}),
]

RAIN_PARAMETER = (
    "tp", {"parameter": "APCP"}
)

"""
1 116.14444 39.91056 Shougang1 A1105 90.0
"""

LOCATION = dict(
    longitude=116.14444,
    latitude=39.91056,
    name="Shougang1",
    id="A1105",
    height=90,
)


def main():
    logger.info("program begin")
    start_time = pd.to_datetime("2023-04-29 00:00:00")
    record_list = []

    forecast_time_list = pd.to_timedelta(np.arange(3, 73, 3), unit="h")
    member_list = np.arange(0, 15)
    for forecast_time in tqdm(forecast_time_list):
        previous_forecast_time = forecast_time - pd.Timedelta(hours=3)
        for number in member_list:
            record = dict(
                forecast_time=forecast_time,
                number=number
            )
            file_path = find_local_file(
                "grapes_reps/grib2/orig",
                start_time=start_time,
                forecast_time=forecast_time,
                number=number
            )
            # print(file_path)
            for name, p in PARAMETER_LIST:
                f = load_field_from_file(
                    file_path,
                    **p,
                )
                if f is None:
                    raise ValueError(f"field not found: {p}")
                p = extract_point(
                    f,
                    latitude=LOCATION["latitude"],
                    longitude=LOCATION["longitude"]
                )
                record[name] = p.values

            previous_file_path = find_local_file(
                "grapes_reps/grib2/orig",
                start_time=start_time,
                forecast_time=previous_forecast_time,
                number=number
            )
            f = load_field_from_file(
                file_path,
                **RAIN_PARAMETER[1],
            )
            f_3 = load_field_from_file(
                previous_file_path,
                **RAIN_PARAMETER[1],
            )
            p = extract_point(
                f - f_3,
                latitude=LOCATION["latitude"],
                longitude=LOCATION["longitude"]
            )
            record[RAIN_PARAMETER[0]] = p.values
            record_list.append(record)

    forecast_time_list = pd.to_timedelta(np.arange(78, 241, 6), unit="h")
    member_list = np.arange(0, 31)
    for forecast_time in tqdm(forecast_time_list):
        previous_forecast_time = forecast_time - pd.Timedelta(hours=6)
        for number in member_list:
            record = dict(
                forecast_time=forecast_time,
                number=number
            )
            file_path = find_local_file(
                "grapes_geps/grib2/orig",
                start_time=start_time,
                forecast_time=forecast_time,
                number=number
            )
            # print(file_path)
            for name, p in PARAMETER_LIST:
                f = load_field_from_file(
                    file_path,
                    **p,
                )
                if f is None:
                    raise ValueError(f"field not found: {p}")
                p = extract_point(
                    f,
                    latitude=LOCATION["latitude"],
                    longitude=LOCATION["longitude"]
                )
                record[name] = p.values

            previous_file_path = find_local_file(
                "grapes_geps/grib2/orig",
                start_time=start_time,
                forecast_time=previous_forecast_time,
                number=number
            )
            f = load_field_from_file(
                file_path,
                **RAIN_PARAMETER[1],
            )
            f_6 = load_field_from_file(
                previous_file_path,
                **RAIN_PARAMETER[1],
            )
            p = extract_point(
                f - f_6,
                latitude=LOCATION["latitude"],
                longitude=LOCATION["longitude"]
            )
            record[RAIN_PARAMETER[0]] = p.values
            record_list.append(record)

    df = pd.DataFrame(record_list)
    # print(df)
    logger.info("program end")


if __name__ == "__main__":
    main()
