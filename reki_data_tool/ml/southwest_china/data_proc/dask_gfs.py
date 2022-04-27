from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import eccodes

import dask
from dask.distributed import Client, progress

from reki.data_finder import find_local_file
from reki.format.grib.eccodes import load_message_from_file
from reki_data_tool.utils import cal_run_time
from reki_data_tool.ml.southwest_china.data_proc.common import get_gfs_parameters

from reki_data_tool.ml.southwest_china.config import OUTPUT_BASE_DIRECTORY


def create_serial(
        start_time: pd.Timestamp,
        forecast_time: pd.Timedelta,
        output_file_path: Union[Path, str]
):
    parameters = get_gfs_parameters()

    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )

    with open(output_file_path, "wb") as f:
        for p in parameters:
            m = load_message_from_file(file_path, **p)
            if m is None:
                continue

            b = eccodes.codes_get_message(m)
            eccodes.codes_release(m)
            f.write(b)
    return True


@cal_run_time
def main():
    date_list = pd.date_range(
        "2021-01-01 00:00", "2021-12-31 23:59",
        freq="12h",
    )
    forecast_hours = np.arange(0, 37, 6)

    client = Client(
        threads_per_worker=1,
        n_workers=32,
    )
    print(client)

    tasks = []
    for date in date_list:
        for hour in forecast_hours:
            start_time = date
            start_time_label = start_time.strftime("%Y%m%d%H")
            forecast_time = pd.Timedelta(hours=hour)
            forecast_time_label = f"{int(forecast_time / pd.Timedelta(hours=1)):03}"

            output_file_name = f"gfs_global_{start_time_label}_{forecast_time_label}.grb2"
            output_file_path = Path(OUTPUT_BASE_DIRECTORY, "gfs", output_file_name)

            t = dask.delayed(create_serial)(start_time, forecast_time, output_file_path)
            tasks.append(t)

    def get_tasks(tasks):
        return tasks

    t = dask.delayed(get_tasks)(tasks)

    result = t.persist()

    progress(result)

    r = result.compute()

    client.close()


if __name__ == "__main__":
    main()
