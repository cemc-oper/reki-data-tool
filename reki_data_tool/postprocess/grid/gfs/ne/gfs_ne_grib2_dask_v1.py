"""
并行方式生成 grib2-ne 数据

1. 并行解码，抽取区域，编码
2. 主程序串行接收编码后的 GRIB 2 消息字节码，写入到文件中

需要传输 GRIB 2 字节码

耗时：1 分钟 (登录节点测试)
"""
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import xarray as xr

from loguru import logger
import eccodes
from tqdm.auto import tqdm

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
    message = extract_region(
        message,
        0, 180, 89.875, 0.125
    )
    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return message_bytes


@cal_run_time
def create_grib2_ne_dask_v1(
        start_time: pd.Timestamp,
        forecast_time: pd.Timedelta,
        output_file_path: Union[Path, str],
        engine: str = "local",
):
    logger.info(f"create dask client with engine {engine}...")
    if engine == "local":
        client = Client(
            threads_per_worker=1,
        )
        logger.info("use local engine")
    elif engine == "mpi":
        from dask_mpi import initialize

        initialize(
            interface="ib0",
            dashboard=False,
            nthreads=1
        )
        client = Client()
        logger.info("use mpi engine")
    else:
        raise ValueError(f"engine is not support: {engine}")

    print(client)
    logger.info("create dask client...done")

    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )
    logger.info(file_path)

    logger.info("count...")
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    logger.info("count..done")

    # 一次性分发任务，Future 按顺序保存到列表中
    logger.info("submit jobs...")
    bytes_futures = []
    for i in range(1, total_count+1):
        f = client.submit(get_message_bytes, file_path, i)
        bytes_futures.append(f)
    logger.info("submit jobs...done")

    # 依次读取 Future 返回值，写入到文件中
    logger.info("receive results and write to file...")
    with open(output_file_path, "wb") as f:
        for i, fut in enumerate(bytes_futures):
            message_bytes = client.gather(fut)
            del fut
            logger.info(f"writing message...{i + 1}/{total_count}")
            f.write(message_bytes)
            del message_bytes
    logger.info("receive results and write to file...done")

    logger.info("close client...")
    client.close()
    logger.info("close client...done")


if __name__ == "__main__":
    from reki_data_tool.postprocess.grid.gfs.ne.config import (
        get_random_start_time,
        get_random_forecast_time,
        OUTPUT_DIRECTORY
    )

    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{int(forecast_time / pd.Timedelta(hours=1)):03}"
    print(start_time_label, forecast_time_label)

    output_directory = OUTPUT_DIRECTORY
    output_file_path = Path(
        output_directory,
        f'ne_{start_time_label}_{forecast_time_label}.grb2'
    )
    print(output_file_path)

    create_grib2_ne_dask_v1(start_time, forecast_time, output_file_path)
