"""
串行方式生成 grib2-ne 数据

5 - 6 分钟
"""
from pathlib import Path
from typing import Union

import pandas as pd
from loguru import logger
from tqdm.auto import tqdm

from reki.data_finder import find_local_file

from reki_data_tool.postprocess.grid.gfs.ne.common import get_message_bytes
from reki_data_tool.utils import cal_run_time, get_message_count


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
    total_count = get_message_count(file_path)
    logger.info("count..done")

    logger.info("process...")
    with open(output_file_path, "wb") as f:
        for i in tqdm(range(1, total_count+1)):
            message_bytes = get_message_bytes(file_path, count=i)
            f.write(message_bytes)
            del message_bytes
    logger.info("process...done")


if __name__ == "__main__":
    from reki_data_tool.postprocess.grid.gfs.ne.config import OUTPUT_DIRECTORY
    from reki_data_tool.postprocess.grid.gfs.util import get_random_start_time, get_random_forecast_time

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
