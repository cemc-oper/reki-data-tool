from pathlib import Path
from typing import Union

import pandas as pd
from loguru import logger
from dask.distributed import Client

from reki.data_finder import find_local_file
from reki_data_tool.utils import cal_run_time
from reki_data_tool.postprocess.grid.gfs.wxzx.common import get_parameters, get_message_bytes


@cal_run_time
def create_wxzx_dask_v1(
        start_time: pd.Timestamp,
        forecast_time: pd.Timedelta,
        output_file_path: Union[Path, str]
):
    logger.info("program begin")
    parameters = get_parameters()

    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )

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
    from reki_data_tool.postprocess.grid.gfs.wxzx.config import OUTPUT_BASE_DIRECTORY
    from reki_data_tool.postprocess.grid.gfs.util import get_random_start_time, get_random_forecast_time

    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{forecast_time/pd.Timedelta(hours=1):03}"
    print(start_time_label, forecast_time_label)

    output_directory = Path(OUTPUT_BASE_DIRECTORY, "11-dask-v1")
    output_file_path = Path(
        output_directory,
        f'wxzx_dask_v1_{start_time_label}_{forecast_time_label}.grb2'
    )
    print(output_file_path)

    create_wxzx_dask_v1(start_time, forecast_time, output_file_path)
