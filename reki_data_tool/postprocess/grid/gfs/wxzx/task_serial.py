from pathlib import Path
from typing import Union

import pandas as pd

from tqdm.auto import tqdm
from loguru import logger

from reki.data_finder import find_local_file
from reki_data_tool.utils import cal_run_time
from reki_data_tool.postprocess.grid.gfs.wxzx.common import get_parameters, get_message_bytes


@cal_run_time
def make_wxzx_data_serial(
        input_file_path: Union[Path, str],
        output_file_path: Union[Path, str]
):
    logger.info("program begin")
    parameters = get_parameters()

    with open(output_file_path, "wb") as f:
        for p in tqdm(parameters):
            b = get_message_bytes(input_file_path, p)
            f.write(b)
    logger.info("program done")


if __name__ == "__main__":
    from reki_data_tool.postprocess.grid.gfs.wxzx.config import OUTPUT_BASE_DIRECTORY
    from reki_data_tool.postprocess.grid.gfs.util import get_random_start_time, get_random_forecast_time

    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{forecast_time/pd.Timedelta(hours=1):03}"
    print(start_time_label, forecast_time_label)

    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )
    print(file_path)

    output_directory = Path(OUTPUT_BASE_DIRECTORY, "02-serial")
    output_file_path = Path(
        output_directory,
        f'wxzx_{start_time_label}_{forecast_time_label}.grb2'
    )
    print(output_file_path)

    make_wxzx_data_serial(file_path, output_file_path)
