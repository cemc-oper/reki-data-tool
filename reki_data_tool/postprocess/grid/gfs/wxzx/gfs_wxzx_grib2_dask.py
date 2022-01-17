from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

import eccodes
from loguru import logger

import dask
from dask.distributed import Client, progress

from reki.data_finder import find_local_file
from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import interpolate_grid


VAR_GROUP_1 = [
    {"parameter": "HGT"},
    {"parameter": "TMP"},
    {"parameter": "UGRD"},
    {"parameter": "VGRD"},
    {"parameter": "DZDT"},
    {"parameter": "RELV"},
    {"parameter": "RELD"},
    {"parameter": "SPFH"},
    {"parameter": "RH"}
]

LEVEL_GROUP_1 = [
    0.1, 0.2, 0.5, 1, 1.5, 2, 3, 4, 5, 7,
    10, 20, 30, 50, 70, 100, 150,
    200, 250, 300, 350, 400, 450, 500,
    550, 600, 650, 700, 750, 800, 850,
    900, 925, 950, 975, 1000
]

VAR_GROUP_2 = [
    {"parameter": "10u"},
    {"parameter": "10v"},
    {"parameter": "2t"},
    {"parameter": "t", "level_type": "surface"},
    {"parameter": "PRMSL"},
    {"parameter": "PRES", "level_type": "surface"},
    {"parameter": "2r"},
    {
        "parameter": "t",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0, "second_level": 0.1}
    },
    {
        "parameter": "t",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0.1, "second_level": 0.3}
    },
    {
        "parameter": "t",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0.3, "second_level": 0.6}
    },
    {
        "parameter": "t",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0.6, "second_level": 1}
    },
    {
        "parameter": "SPFH",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0, "second_level": 0.1}
    },
    {
        "parameter": "SPFH",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0.1, "second_level": 0.3}
    },
    {
        "parameter": "SPFH",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0.3, "second_level": 0.6}
    },
    {
        "parameter": "SPFH",
        "level_type": "depthBelowLandLayer",
        "level": {"first_level": 0.6, "second_level": 1}
    },
    {"parameter": "ACPCP"},
    {"parameter": "NCPCP"},
    {"parameter": "APCP"},
    {"parameter": "LCDC"},
    {"parameter": "MCDC"},
    {"parameter": "HCDC"},
    {"parameter": "TCDC"},
    {"parameter": "2t"},
    {"parameter": "2t"},
    {"parameter": "LHTFL"},
    {"parameter": "LHTFL"},
    {"parameter": "NSWRF"},
    {"parameter": "NLWRF"},
    {"parameter": "SNOD"},
    {"parameter": "SNOD"},
    {"parameter": "ALBDO"},
    {"parameter": "SNOD"},
    {"parameter": "SNOD"},
    {"parameter": "HGT", "level_type": "surface"},
    {"parameter": "HPBL", "level_type": "surface"}
]


VAR_GROUP_3 = [
    {"parameter": "DPT"},
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 225,
        }
    },
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 224,
        }
    },
]

LEVEL_GROUP_3 = [
    200, 250, 300, 350, 400, 450, 500,
    550, 600, 650, 700, 750, 800, 850,
    900, 925, 950, 975, 1000
]


VAR_GROUP_4 = [
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 0,
            "parameterNumber": 224,
        }
    },
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 2,
            "parameterNumber": 224,
        }
    },
]

LEVEL_GROUP_4 = [
    200, 500, 700, 850, 925, 1000
]


VAR_GROUP_5 = [
    {"parameter": "DEPR"},
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 224,
        }
    },
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 225,
        }
    },
    {"parameter": "EPOT"}
]

LEVEL_GROUP_5 = [
    500, 700, 850, 925
]


VAR_GROUP_6 = [
    {"parameter": "2t"}
]


def get_parameters():
    parameters = []
    for variable in VAR_GROUP_1:
        for level in LEVEL_GROUP_1:
            parameters.append({
                **variable,
                "level": level,
                "level_type": "pl"
            })

    parameters.extend(VAR_GROUP_2)

    for variable in VAR_GROUP_3:
        for level in LEVEL_GROUP_3:
            parameters.append({
                **variable,
                "level": level,
                "level_type": "pl"
            })

    for variable in VAR_GROUP_4:
        for level in LEVEL_GROUP_4:
            parameters.append({
                **variable,
                "level": level,
                "level_type": "pl"
            })

    for variable in VAR_GROUP_5:
        for level in LEVEL_GROUP_5:
            parameters.append({
                **variable,
                "level": level,
                "level_type": "pl"
            })

    parameters.extend(VAR_GROUP_6)

    return parameters


def get_message_bytes(file_path, record):
    m = load_message_from_file(file_path, **record)
    if m is None:
        return None

    m = interpolate_grid(
        m,
        latitude=np.arange(45, -45, -0.28125),
        longitude=np.arange(0, 360, 0.28125),
        # bounds_error=False,
        # fill_value=None,
    )

    message_bytes = eccodes.codes_get_message(m)
    eccodes.codes_release(m)
    return message_bytes


def main():
    logger.info("program begin")
    parameters = get_parameters()

    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=pd.to_datetime("2021-09-08 00:00:00"),
        forecast_time=pd.to_timedelta("24h")
    )

    output_directory = "/g11/wangdp/project/work/data/playground/operation/gfs/wxzx/output"
    output_file_path = Path(output_directory, "dask_wxzx.grb2")

    client = Client(
        threads_per_worker=1,
    )
    print(client)

    bytes_futures = []
    for record in parameters:
        f = client.submit(get_message_bytes, file_path, record)
        bytes_futures.append(f)

    # def get_object(l):
    #     return l
    #
    # bytes_lists = dask.delayed(get_object)(bytes_futures)
    # f = bytes_lists.persist()
    # progress(f)
    # bytes_futures = f.compute()

    total_count = len(parameters)
    with open(output_file_path, "wb") as f:
        for i, fut in enumerate(bytes_futures):
            message_bytes = client.gather(fut)
            del fut
            logger.info(f"writing message...{i + 1}/{total_count}")
            if message_bytes is not None:
                f.write(message_bytes)
                del message_bytes

    client.close()

    logger.info("program done")


if __name__ == "__main__":
    main()
