"""
并行方式生成 grib2-ne 数据

分批解码、编码、写文件，每次分发 32 个任务

是否对多计算节点有帮助？
"""

from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from loguru import logger
import eccodes

import dask
from dask.distributed import Client, progress

from reki.data_finder import find_local_file
from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region

from reki_data_tool.postprocess.grid.gfs.ne.config import START_TIME, FORECAST_TIME, OUTPUT_DIRECTORY
from reki_data_tool.utils import cal_run_time


def get_message_bytes(
        file_path,
        count,
) -> bytes:
    message = load_message_from_file(file_path, count=count)
    extract_region(
        message,
        0, 180, 89.875, 0.125
    )
    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return message_bytes
    # return b'1'


@cal_run_time
def create_grib2_ne_dask_v2():
    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=START_TIME,
        forecast_time=FORECAST_TIME
    )

    output_directory = OUTPUT_DIRECTORY
    output_file_path = Path(output_directory, "ne_dask.grb2")

    logger.info("count...")
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    logger.info("count..done")

    client = Client(
        threads_per_worker=1,
    )
    print(client)

    def get_object(x):
        return x

    with open(output_file_path, "wb") as f:
        for batch in np.array_split(np.arange(1, total_count+1), np.ceil(total_count/32)):
            bytes_lazy = []
            for i in batch:
                fut = dask.delayed(get_message_bytes)(file_path, i)
                bytes_lazy.append(fut)
            b = dask.delayed(get_object)(bytes_lazy)
            b_future = b.persist()
            bytes_result = b_future.compute()
            del b_future

            for i, b in enumerate(bytes_result):
                logger.info(f"writing message...{i + batch[0]}/{total_count}")
                f.write(b)
                del b

    client.close()


if __name__ == "__main__":
    create_grib2_ne_dask_v2()
