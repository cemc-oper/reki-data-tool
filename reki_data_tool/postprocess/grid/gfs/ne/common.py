from typing import Union, Optional
from pathlib import Path

import eccodes
import numpy as np

from reki.format.grib.common import MISSING_VALUE
from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region, interpolate_grid


def get_message_bytes(
        file_path: Union[str, Path],
        start_longitude: Union[float, int],
        end_longitude: Union[float, int],
        longitude_step: Optional[Union[float, int]],
        start_latitude: Union[float, int],
        end_latitude: Union[float, int],
        latitude_step: Optional[Union[float, int]],
        count: int,
) -> bytes:
    """
    从 GRIB2 文件中读取第 count 个要素场，裁剪区域，并返回新场的字节码。
    如果设置了 longitude_step 和 latitude_step，则使用最近邻插值生成数据。

    Parameters
    ----------
    file_path
    start_longitude
    end_longitude
    longitude_step
    start_latitude
    end_latitude
    latitude_step
    count
        要素场序号，从 1 开始，ecCodes GRIB Key count

    Returns
    -------
    bytes
        重新编码后的 GRIB 2 消息字节码
    """
    message = load_message_from_file(file_path, count=count)

    if latitude_step is None and longitude_step is None:
        message = extract_region(
            message,
            # 0, 180, 89.875, 0.125
            start_longitude,
            end_longitude,
            start_latitude,
            end_latitude
        )
    elif latitude_step is not None and longitude_step is not None:
        missing_value = MISSING_VALUE
        message = interpolate_grid(
            message,
            latitude=np.arange(start_latitude, end_latitude + latitude_step, latitude_step),
            longitude=np.arange(start_longitude, end_longitude + longitude_step, longitude_step),
            bounds_error=False,
            fill_value=missing_value,
            scheme="nearest"
        )
    else:
        raise ValueError("longitude_step and latitude_step must be set together")

    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return message_bytes


