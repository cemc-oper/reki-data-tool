"""
并行方式生成 grib2-ne 数据

分批解码、编码、写文件，每次分发 32 个任务

是否对多计算节点有帮助？
"""

from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import dask
from loguru import logger

from reki.data_finder import find_local_file

from reki_data_tool.postprocess.grid.gfs.ne.common import get_message_bytes
from reki_data_tool.utils import cal_run_time, get_message_count, create_dask_client


@cal_run_time
def create_grib2_ne_dask_v2(
        start_time: pd.Timestamp,
        forecast_time: pd.Timedelta,
        output_file_path: Union[Path, str],
        engine: str = "local",
        batch_size: int = 32,
):
    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )

    logger.info("count...")
    total_count = get_message_count(file_path)
    logger.info(f"count..done, {total_count}")

    if engine == "local":
        client_kwargs = dict(nthreads_per_worker=1)
    else:
        client_kwargs = dict()
    client = create_dask_client(engine=engine, client_kwargs=client_kwargs)
    logger.info(f"client: {client}")

    def get_object(x):
        return x

    with open(output_file_path, "wb") as f:
        for batch in np.array_split(np.arange(1, total_count+1), np.ceil(total_count/batch_size)):
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
    from reki_data_tool.postprocess.grid.gfs.ne.config import OUTPUT_DIRECTORY
    from reki_data_tool.postprocess.grid.gfs.util import get_random_start_time, get_random_forecast_time

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

    create_grib2_ne_dask_v2(start_time, forecast_time, output_file_path)
