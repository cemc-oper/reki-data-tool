from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd
from loguru import logger

import dask
from dask.distributed import Client, progress

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


def get_point(file_path, name, field_filter, forecast_time, number):
    f = load_field_from_file(
        file_path,
        **field_filter,
    )
    p = extract_point(
        f,
        latitude=LOCATION["latitude"],
        longitude=LOCATION["longitude"],
        scheme="nearest"
    )
    return {
        "forecast_time": int(forecast_time/pd.Timedelta(hours=1)),
        "number": number,
        "name": name,
        "value": p.values,
    }


def get_rain_point(file_path, previous_file_path, name, field_filter, forecast_time, number):
    f = load_field_from_file(
        file_path,
        **field_filter,
    )
    f_3 = load_field_from_file(
        previous_file_path,
        **field_filter,
    )
    p = extract_point(
        f - f_3,
        latitude=LOCATION["latitude"],
        longitude=LOCATION["longitude"],
        scheme="nearest"
    )
    return {
        "forecast_time": int(forecast_time/pd.Timedelta(hours=1)),
        "number": number,
        "name": name,
        "value": p.values,
    }


def main():
    logger.info("program begin")

    output_directory = "/g11/wangdp/project/work/data/playground/winter/eps_station_fdp/output"
    output_file_name = "station.csv"

    start_time = pd.to_datetime("2023-04-29 00:00:00")

    client = Client(
        threads_per_worker=1
    )

    record_list = []

    forecast_time_list = pd.to_timedelta(np.arange(3, 73, 3), unit="h")
    member_list = np.arange(0, 15)
    for forecast_time in forecast_time_list:
        previous_forecast_time = forecast_time - pd.Timedelta(hours=3)
        for number in member_list:
            file_path = find_local_file(
                "grapes_reps/grib2/orig",
                start_time=start_time,
                forecast_time=forecast_time,
                number=number,
                data_level=("runtime", "archive")
            )

            previous_file_path = find_local_file(
                "grapes_reps/grib2/orig",
                start_time=start_time,
                forecast_time=previous_forecast_time,
                number=number,
                data_level=("runtime", "archive")
            )

            for name, p in PARAMETER_LIST:
                record_future = dask.delayed(get_point)(file_path, name, p, forecast_time, number)
                record_list.append(record_future)

            record = dask.delayed(get_rain_point)(
                file_path, previous_file_path, RAIN_PARAMETER[0], RAIN_PARAMETER[1], forecast_time, number
            )
            record_list.append(record)

    forecast_time_list = pd.to_timedelta(np.arange(78, 241, 6), unit="h")
    member_list = np.arange(0, 31)
    for forecast_time in forecast_time_list:
        previous_forecast_time = forecast_time - pd.Timedelta(hours=6)
        for number in member_list:
            file_path = find_local_file(
                "grapes_geps/grib2/orig",
                start_time=start_time,
                forecast_time=forecast_time,
                number=number,
                data_level=("runtime", "archive")
            )

            previous_file_path = find_local_file(
                "grapes_geps/grib2/orig",
                start_time=start_time,
                forecast_time=previous_forecast_time,
                number=number,
                data_level=("runtime", "archive")
            )

            if file_path is None:
                logger.warning(f"file is not found: {forecast_time} mem {number}")
                continue

            if previous_file_path is None:
                logger.warning(f"previous file is not found: {previous_forecast_time} mem {number}")
                continue

            for name, p in PARAMETER_LIST:
                record_future = dask.delayed(get_point)(file_path, name, p, forecast_time, number)
                record_list.append(record_future)

            record = dask.delayed(get_rain_point)(
                file_path, previous_file_path, RAIN_PARAMETER[0], RAIN_PARAMETER[1], forecast_time, number
            )
            record_list.append(record)

    def get_object(a):
        return a

    rl = dask.delayed(get_object)(record_list)
    r = rl.persist()
    progress(r)
    record_list = r.compute()
    del r
    client.close()

    df = pd.DataFrame(record_list)
    print(df)
    csv_file_path = Path(output_directory, output_file_name)
    logger.info(f"saving table to {csv_file_path}")
    df.to_csv(csv_file_path, index=False)

    logger.info("program end")


if __name__ == "__main__":
    main()
