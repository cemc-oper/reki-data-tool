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
    message = extract_region(
        message,
        101, 115, 42, 29
    )
    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return message_bytes


def main():
    file_path = find_local_file(
        "grapes_tym/grib2/orig",
        start_time=pd.to_datetime("2021-09-26 00:00:00"),
        forecast_time=pd.Timedelta(hours=120)
    )

    output_directory = "/g11/wangdp/project/work/data/playground/operation/tym/shaanxi/output"
    output_file_path = Path(output_directory, "shaanxi_dask.grb2")
    output_file_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("count...")
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    logger.info("count..done")

    client = Client(
        threads_per_worker=1,
    )
    print(client)

    bytes_futures = []
    for i in range(1, total_count+1):
        f = client.submit(get_message_bytes, file_path, i)
        bytes_futures.append(f)

    with open(output_file_path, "wb") as f:
        for i, fut in enumerate(bytes_futures):
            message_bytes = client.gather(fut)
            del fut
            logger.info(f"writing message...{i + 1}/{total_count}")
            f.write(message_bytes)
            del message_bytes

    client.close()


if __name__ == "__main__":
    start_time = pd.Timestamp.now()
    main()
    end_time = pd.Timestamp.now()
    print(end_time - start_time)
