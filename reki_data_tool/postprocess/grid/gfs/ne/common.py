from typing import Union
from pathlib import Path

import eccodes

from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region


def get_message_bytes(
        file_path: Union[str, Path],
        count: int,
) -> bytes:
    """
    从 GRIB2 文件中读取第 count 个要素场，裁剪区域，并返回新场的字节码

    Parameters
    ----------
    file_path
    count
        要素场序号，从 1 开始，ecCodes GRIB Key count

    Returns
    -------
    bytes
    """
    message = load_message_from_file(file_path, count=count)
    message = extract_region(
        message,
        0, 180, 89.875, 0.125
    )
    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return message_bytes
