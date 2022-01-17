"""
冬奥系统 1KM BTH GRIB2 网格产品

简单并行

每个目标文件一个 Dask 任务，任务内部串行从多个文件中顺序抽取要素场
"""
from pathlib import Path

import pandas as pd
import numpy as np
import xarray as xr

import dask
from dask.distributed import Client, progress
from dask_mpi import initialize

from reki.format.grib.eccodes import load_bytes_from_file


PRODUCTION_LIST = [
    {
        "field": {"parameter": "HGT"},
        "output": {"name": "HGT"}
    },
    {
        "field": {"parameter": "2t"},
        "output": {"name": "2T"}
    },
    {
        "field": {"parameter": "2r"},
        "output": {"name": "2RH"}
    },
    {
        "field": {"parameter": "10u"},
        "output": {"name": "10U"}
    },
    {
        "field": {"parameter": "10v"},
        "output": {"name": "10V"}
    },
    {
        "field": {"parameter": "GUST", "level": 10},
        "output": {"name": "10FG1"}
    },
    {
        "field": {"parameter": "PWAT"},
        "output": {"name": "PWV"}
    },
    {
        "field": {"parameter": "VIS"},
        "output": {"name": "VIS"}
    },
    {
        "field": {"parameter": "PRES"},
        "output": {"name": "SP"}
    },
    {
        "field": {"parameter": "t", "level_type": "surface"},
        "output": {"name": "SKT"}
    },
    {
        "field": {"parameter": "TCDC"},
        "output": {"name": "TCC"}
    },
    {
        "field": {"parameter": "LCDC"},
        "output": {"name": "LCC"}
    },
    {
        "field": {"parameter": "APCP"},
        "output": {"name": "TP"}
    },
    {
        "field": {"parameter": "ASNOW"},
        "output": {"name": "SF"}
    },
    {
        "field": {"parameter": "2d"},
        "output": {"name": "2D"}
    },
    {
        "field": {"parameter": {"discipline": 0, "parameterCategory": 3, "parameterNumber": 225}, "level_type": "surface"},
        "output": {"name": "DEG0L"}
    },
    {
        "field": {"parameter": {"discipline": 0, "parameterCategory": 16, "parameterNumber": 224}, "level_type": "surface"},
        "output": {"name": "CRR"}
    },
    {
        "field": {"parameter": "PTYPE"},
        "output": {"name": "PTYPE"}
    },
    *[
        {
            "field": {"parameter": "t", "level_type": "pl", "level": level},
            "output": {"name": f"{level}T"}
        } for level in [925, 850, 800, 700, 500]
    ],
    *[
        {
            "field": {"parameter": "q", "level_type": "pl", "level": level},
            "output": {"name": f"{level}Q"}
        } for level in [925, 850, 800, 700, 500]
    ],
    *[
        {
            "field": {"parameter": "gh", "level_type": "pl", "level": level},
            "output": {"name": f"{level}GH"}
        } for level in [925, 850, 800, 700, 500]
    ],
    *[
        {
            "field": {"parameter": "u", "level_type": "pl", "level": level},
            "output": {"name": f"{level}U"}
        } for level in [925, 850, 800, 700, 500]
    ],
    *[
        {
            "field": {"parameter": "v", "level_type": "pl", "level": level},
            "output": {"name": f"{level}V"}
        } for level in [925, 850, 800, 700, 500]
    ],
    *[
        {
            "field": {"parameter": "wz", "level_type": "pl", "level": level},
            "output": {"name": f"{level}W"}
        } for level in [925, 850, 800, 700, 500]
    ],
    *[
        {
            "field": {"parameter": "r", "level_type": "pl", "level": level},
            "output": {"name": f"{level}R"}
        } for level in [925, 850, 800, 700, 500]
    ],
]


def get_grib2_file_name(start_time: pd.Timestamp, forecast_time: pd.Timedelta):
    start_time_str = start_time.strftime("%Y%m%d%H")
    forecast_time_str = f"{int(forecast_time/pd.Timedelta(hours=1)):03}"
    return f"rmf.hgra.{start_time_str}{forecast_time_str}.grb2"


def get_fields(product, start_time, grib_orig_path, output_path):
    field = product["field"]

    output_name = product["output"]["name"]
    output_file = Path(output_path, f"{output_name}.grb2")

    forecast_time_list = pd.to_timedelta(np.arange(0, 25, 1), unit="h")

    with open(output_file, "wb") as f:
        for forecast_time in forecast_time_list:
            file_name = get_grib2_file_name(start_time, forecast_time)
            # print(file_name)
            field_bytes = load_bytes_from_file(
                Path(grib_orig_path, file_name),
                **field
            )
            f.write(field_bytes)
            del field_bytes
    return output_file


def main():
    start_time = "2021083100"

    grib_orig_path = "/g11/wangdp/project/work/data/playground/winter/grid/data/grib2-orig"
    output_path = "/g11/wangdp/project/work/data/playground/winter/grid/output/serial"

    client = Client()

    start_time = pd.to_datetime(start_time, format="%Y%m%d%H")

    output_files = []
    for product in PRODUCTION_LIST:
        # print(product)
        output_file_path = dask.delayed(get_fields)(product, start_time, grib_orig_path, output_path)
        output_files.append(output_file_path)

    p = client.compute(output_files)
    progress(p)

    output_f = client.gather(p)
    print(output_f)
    del p

    client.close()


if __name__ == "__main__":
    print(pd.Timestamp.now())
    main()
    print(pd.Timestamp.now())