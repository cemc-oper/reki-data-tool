from pathlib import Path
from typing import Union

from tqdm.auto import tqdm
from loguru import logger

from reki_data_tool.utils import cal_run_time
from reki_data_tool.postprocess.grid.gfs.abroad.common import get_parameters, get_message_bytes


@cal_run_time
def make_abroad_data_serial(
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