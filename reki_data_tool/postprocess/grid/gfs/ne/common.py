from typing import Union
from pathlib import Path

import eccodes

from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region


def get_message_bytes(
        file_path: Union[str, Path],
        start_longitude: Union[float, int],
        end_longitude: Union[float, int],
        start_latitude: Union[float, int],
        end_latitude: Union[float, int],
        count: int,
) -> bytes:
    """
    从 GRIB2 文件中读取第 count 个要素场，裁剪区域，并返回新场的字节码

    Parameters
    ----------
    file_path
    start_longitude
    end_longitude
    start_latitude
    end_latitude
    count
        要素场序号，从 1 开始，ecCodes GRIB Key count

    Returns
    -------
    bytes
        重新编码后的 GRIB 2 消息字节码
    """
    message = load_message_from_file(file_path, count=count)
    message = extract_region(
        message,
        # 0, 180, 89.875, 0.125
        start_longitude,
        end_longitude,
        start_latitude,
        end_latitude
    )
    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return message_bytes
