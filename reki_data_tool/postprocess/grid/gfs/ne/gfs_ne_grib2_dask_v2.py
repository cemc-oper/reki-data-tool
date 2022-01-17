from pathlib import Path

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


def get_message_bytes(
        file_path,
        count,
) -> bytes:
    message = load_message_from_file(file_path, count=count)
    # extract_region(
    #     message,
    #     0, 180, 89.875, 0.125
    # )
    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return b'1'


def main():
    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=pd.to_datetime("2021-09-01 00:00:00"),
        forecast_time=pd.Timedelta(hours=24)
    )

    output_directory = "/g11/wangdp/project/work/data/playground/operation/gfs/ne/output"
    output_file_path = Path(output_directory, "ne_dask.grb2")

    logger.info("count...")
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    logger.info("count..done")

    client = Client(
        threads_per_worker=1,
    )
    print(client)

    def get_object(x):
        return x

    with open(output_file_path, "wb") as f:
        for batch in np.array_split(np.arange(1, total_count+1), np.ceil(total_count/32)):
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
    main()
