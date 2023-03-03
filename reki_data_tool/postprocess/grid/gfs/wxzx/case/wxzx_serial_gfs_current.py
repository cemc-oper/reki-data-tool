from pathlib import Path

import pandas as pd

from reki.data_finder import find_local_file
from reki_data_tool.postprocess.grid.gfs.wxzx.task_serial import make_wxzx_data_serial
from reki_data_tool.postprocess.grid.gfs.wxzx.config import OUTPUT_BASE_DIRECTORY
from reki_data_tool.postprocess.grid.gfs.util import get_random_start_time, get_random_forecast_time


def main():
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{forecast_time / pd.Timedelta(hours=1):03}"
    print(start_time_label, forecast_time_label)

    input_file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )
    print(input_file_path)

    output_directory = Path(OUTPUT_BASE_DIRECTORY, "11-dask-v1")
    output_file_path = Path(
        output_directory,
        f'wxzx_gfs_4_0_dask_v1_{start_time_label}_{forecast_time_label}.grb2'
    )
    print(output_file_path)

    make_wxzx_data_serial(input_file_path, output_file_path)


if __name__ == "__main__":
    main()
