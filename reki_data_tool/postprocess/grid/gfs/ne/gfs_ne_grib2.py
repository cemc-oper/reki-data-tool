"""
串行方式生成 grib2-ne 数据

5 - 6 分钟
"""
from pathlib import Path

from loguru import logger
import eccodes
from tqdm.auto import tqdm

from reki.data_finder import find_local_file
from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region

from reki_data_tool.postprocess.grid.gfs.ne.config import START_TIME, FORECAST_TIME, OUTPUT_DIRECTORY
from reki_data_tool.utils import cal_run_time


@cal_run_time
def main():
    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=START_TIME,
        forecast_time=FORECAST_TIME
    )

    output_directory = OUTPUT_DIRECTORY
    output_file_path = Path(output_directory, "ne.grb2")

    logger.info("count...")
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    logger.info("count..done")

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


if __name__ == "__main__":
    main()
