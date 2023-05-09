from pathlib import Path

import pandas as pd

from reki_data_tool.postprocess.grid.gfs.wxzx.task_serial import make_wxzx_data_serial
from reki_data_tool.postprocess.grid.gfs.wxzx.config import OUTPUT_BASE_DIRECTORY


def main():
    start_time = pd.to_datetime("2023-04-19 12:00")
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = pd.to_timedelta("6h")
    forecast_time_label = f"{int(forecast_time/pd.Timedelta(hours=1)):03}"

    input_file_path = (
        f"/g0/nwp_pd/NWP_CMA_GFS_GMF_POST_V2023_DATA/"
        f"{start_time_label}/data/output/grib2_orig/gmf.gra.{start_time_label}{forecast_time_label}.grb2"
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
