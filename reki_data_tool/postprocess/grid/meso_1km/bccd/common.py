from typing import List, Optional, Union, Dict
from pathlib import Path

import eccodes

from reki.format.grib.eccodes import load_message_from_file


LEVEL_LIST_1 = sorted([
    100, 150,200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 925, 950, 975, 1000
], reverse=True)


LEVEL_LIST_2 = [
    30, 50, 70, 100, 120, 140, 160, 180, 200
]


PARAM_LIST = [
    *[
        {"parameter": "UGRD", "level_type": "pl", "level": level} for level in LEVEL_LIST_1
    ],
    *[
        {"parameter": "VGRD", "level_type": "pl", "level": level} for level in LEVEL_LIST_1
    ],
    {"parameter": "UGRD", "level_type": "heightAboveGround", "level": 10},
    {"parameter": "VGRD", "level_type": "heightAboveGround", "level": 10},
    {"parameter": "TMP", "level_type": "heightAboveGround", "level": 2},
    {"parameter": "TMP", "level_type": "surface", "level": 0},
    {"parameter": "PRMSL", "level_type": "meanSea"},
    {"parameter": "PRES", "level_type": "surface"},
    {"parameter": "RH", "level_type": "heightAboveGround", "level": 2},
    {"parameter": "APCP"},
    {"parameter": "HGT", "level_type": "surface"},
    {"parameter": "CAPE"},
    {"parameter": "KX"},
    {"parameter": {"discipline": 0, "parameterCategory": 16, "parameterNumber": 224}, "level_type": "surface"},
    {"parameter": "BLI"},
    {"parameter": {"discipline": 0, "parameterCategory": 1, "parameterNumber": 239}, "level_type": "surface"},
    {"parameter": "DPT", "level_type": "heightAboveGround", "level": 2},
    {"parameter": "UPHL", "level_type": "heightAboveGroundLayer", "level": {"first_level": 5000, "second_level": 2000}},
    {"parameter": "PWAT"},
    {
        "parameter": {"discipline": 0, "parameterCategory": 2, "parameterNumber": 237},
        "level_type": "heightAboveGroundLayer",
        "level": {"first_level": 600, "second_level": 0},
    },
    {"parameter": "VIS", "level_type": "surface"},
    {"parameter": "GUST", "level_type": "heightAboveGround", "level": 10},
    *[
        {"parameter": "UGRD", "level_type": "heightAboveGround", "level": level} for level in LEVEL_LIST_2
    ],
    *[
        {"parameter": "VGRD", "level_type": "heightAboveGround", "level": level} for level in LEVEL_LIST_2
    ],
]


def get_message_bytes(file_path: Union[Path, str], record: Dict) -> Optional[bytes]:
    input_record = record
    m = load_message_from_file(file_path, **input_record)
    if m is None:
        print(f"record is not found: {record}")
        return None

    message_bytes = eccodes.codes_get_message(m)
    eccodes.codes_release(m)
    return message_bytes
