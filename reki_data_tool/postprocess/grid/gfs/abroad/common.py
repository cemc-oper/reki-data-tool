from typing import List, Optional, Union, Dict
from pathlib import Path

import eccodes
import numpy as np

from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.common import MISSING_VALUE
from reki.format.grib.eccodes.operator import interpolate_grid

VAR_GROUP_1 = [
    {"parameter": "HGT"},
    {"parameter": "TMP"},
    {"parameter": "UGRD"},
    {"parameter": "VGRD"},
    {"parameter": "DZDT"},
    {"parameter": "SPFH"},
    {"parameter": "RH"},
]

LEVEL_GROUP_1 = [
    200, 300, 400, 500, 600, 700, 850, 925, 1000
]

VAR_GROUP_2 = [
    {"parameter": "RELV"},
    {"parameter": "RELD"},
]

LEVEL_GROUP_2 = [
    200, 500, 700, 850
]

VAR_GROUP_3 = [
    {"parameter": {"discipline": 0, "parameterCategory": 0, "parameterNumber": 224}},
    {"parameter": {"discipline": 0, "parameterCategory": 2, "parameterNumber": 224}},
    {"parameter": {"discipline": 0, "parameterCategory": 1, "parameterNumber": 224}},
    {"parameter": {"discipline": 0, "parameterCategory": 1, "parameterNumber": 225}},
    {"parameter": "DEPR"},
    {"parameter": "EPOT"},
]

LEVEL_GROUP_3 = [
    500, 700, 850
]

VAR_GROUP_4 = [
    {"input": {"parameter": "10u"}},
    {"input": {"parameter": "10v"}},
    {"input": {"parameter": "2t"}},
    {"input": {"parameter": "2r"}},
    {"input": {"parameter": "PRMSL"}},
    {"input": {"parameter": "PRES", "level_type": "surface"}},
    {"input": {"parameter": "APCP", "level_type": "surface"}},
    {"input": {"parameter": "KX"}},
]


def get_parameters() -> List:
    parameters = []
    for variable in VAR_GROUP_1:
        for level in LEVEL_GROUP_1:
            param = {
                "input": {
                    **variable,
                    "level": level,
                    "level_type": "pl"
                }
            }
            parameters.append(param)

    for variable in VAR_GROUP_2:
        for level in LEVEL_GROUP_2:
            param = {
                "input": {
                    **variable,
                    "level": level,
                    "level_type": "pl"
                }
            }
            parameters.append(param)

    for variable in VAR_GROUP_3:
        for level in LEVEL_GROUP_3:
            param = {
                "input": {
                    **variable,
                    "level": level,
                    "level_type": "pl"
                }
            }
            parameters.append(param)

    parameters.extend(VAR_GROUP_4)

    return parameters


def get_message_bytes(file_path: Union[Path, str], record: Dict) -> Optional[bytes]:
    input_record = record["input"]
    m = load_message_from_file(file_path, **input_record)
    if m is None:
        print(f"record is not found: {record}")
        return None

    missing_value = MISSING_VALUE

    m = interpolate_grid(
        m,
        latitude=np.arange(90, -90 - 1, -1),
        longitude=np.arange(0, 360, 1),
        bounds_error=False,
        fill_value=missing_value,
    )

    message_bytes = eccodes.codes_get_message(m)
    eccodes.codes_release(m)
    return message_bytes
