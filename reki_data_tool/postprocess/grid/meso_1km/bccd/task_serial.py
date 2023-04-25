from pathlib import Path
from typing import Union

import pandas as pd

from tqdm.auto import tqdm
from loguru import logger

from reki_data_tool.utils import cal_run_time
from reki_data_tool.postprocess.grid.meso_1km.bccd.common import PARAM_LIST, get_message_bytes


@cal_run_time
def make_bccd_data_serial(
        input_file_path: Union[Path, str],
        output_file_path: Union[Path, str]
):
    logger.info("program begin")
    parameters = PARAM_LIST

    with open(output_file_path, "wb") as f:
        for p in tqdm(parameters):
            b = get_message_bytes(input_file_path, p)
            f.write(b)
    logger.info("program done")


if __name__ == "__main__":
    from reki_data_tool.postprocess.grid.meso_1km.bccd.config import OUTPUT_BASE_DIRECTORY

    start_time_label = "2023042000"
    forecast_time_label = "003"
    print(start_time_label, forecast_time_label)

    file_path = "/g2/nwp_sp/NWP_CMA_MESO_1KM_POST_DATA/2023042000/data/output/grib2-orig/rmf.hgra.2023042000025.grb2"
    print(file_path)

    output_directory = Path(OUTPUT_BASE_DIRECTORY, "02-serial")
    output_file_path = Path(
        output_directory,
        f'bccd_{start_time_label}_{forecast_time_label}.grb2'
    )
    print(output_file_path)

    make_bccd_data_serial(file_path, output_file_path)
