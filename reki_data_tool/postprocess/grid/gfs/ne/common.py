from typing import Union
from pathlib import Path

import eccodes

from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import extract_region


def get_message_bytes(
        file_path: Union[str, Path],
        count: int,
) -> bytes:
    message = load_message_from_file(file_path, count=count)
    message = extract_region(
        message,
        0, 180, 89.875, 0.125
    )
    message_bytes = eccodes.codes_get_message(message)
    eccodes.codes_release(message)
    return message_bytes
