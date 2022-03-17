from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd

import eccodes
from tqdm.auto import tqdm
from loguru import logger

from reki.data_finder import find_local_file
from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import interpolate_grid
from reki_data_tool.utils import cal_run_time
from reki_data_tool.postprocess.grid.gfs.wxzx.common import get_parameters


@cal_run_time
def create_wxzx_serial(
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

    with open(output_file_path, "wb") as f:
        for p in tqdm(parameters):
            m = load_message_from_file(file_path, **p)
            if m is None:
                continue

            interpolate_grid(
                m,
                latitude=np.arange(45, -45, -0.28125),
                longitude=np.arange(0, 360, 0.28125),
                # bounds_error=False,
                # fill_value=None,
            )

            b = eccodes.codes_get_message(m)
            eccodes.codes_release(m)
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

    output_directory = Path(OUTPUT_BASE_DIRECTORY, "02-serial")
    output_file_path = Path(
        output_directory,
        f'wxzx_{start_time_label}_{forecast_time_label}.grb2'
    )
    print(output_file_path)

    create_wxzx_serial(start_time, forecast_time, output_file_path)
