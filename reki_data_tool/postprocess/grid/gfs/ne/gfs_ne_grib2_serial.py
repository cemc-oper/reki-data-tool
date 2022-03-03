"""
串行方式生成 grib2-ne 数据

5 - 6 分钟
"""
from pathlib import Path
from typing import Union

import pandas as pd
from loguru import logger
import eccodes
from tqdm.auto import tqdm

from reki.data_finder import find_local_file
from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region

from reki_data_tool.utils import cal_run_time


@cal_run_time
def create_grib2_ne(
        start_time: pd.Timestamp,
        forecast_time: pd.Timedelta,
        output_file_path: Union[Path, str]
):
    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )

    logger.info("count...")
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    logger.info("count..done")

    logger.info("process...")
    with open(output_file_path, "wb") as f:
        for i in tqdm(range(1, total_count+1)):
            message = load_message_from_file(file_path, count=i)
            message = extract_region(
                message,
                0, 180, 89.875, 0.125
            )
            message_bytes = eccodes.codes_get_message(message)
            f.write(message_bytes)
            eccodes.codes_release(message)
    logger.info("process...done")


if __name__ == "__main__":
    from reki_data_tool.postprocess.grid.gfs.ne.config import (
        get_random_start_time,
        get_random_forecast_time,
        OUTPUT_DIRECTORY
    )

    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{forecast_time/pd.Timedelta(hours=1):03}"
    print(start_time_label, forecast_time_label)

    output_directory = OUTPUT_DIRECTORY
    output_file_path = Path(
        output_directory,
        f'ne_{start_time_label}_{forecast_time_label}.grb2'
    )
    print(output_file_path)

    create_grib2_ne(start_time, forecast_time, output_file_path)
