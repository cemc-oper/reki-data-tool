from typing import List, Optional, Union, Dict
from pathlib import Path

import eccodes
import numpy as np

from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.common import MISSING_VALUE
from reki.format.grib.eccodes.operator import interpolate_grid


"""
./gribpost.exe -s $input_file | grep -E ":H:|:T:|:U:|:V:|:W:|:DIV:|:VOR:|:RH:|:PS:|:PSL:|:RAINC:|:RAINNC:|:RAIN:|:TS:|:T2M:|:CC:|:TCC:|:LCC:|:MCC:|:HCC:|:SF:|:SD:|:KIDX:|:ZS:|CAPE:|:CIN:|:PLI:|:PC:|:BTI:|:BTV:|:C_DBZ:|:PBLH:|:SWEATIDX:|:DBZ:|:GRAUPEL:|:QS:|:QI:|:QR:|:QC:|:Q:|:ALBEDO:|:ZNT:|:SI:|:TVW:|:TCW:|:TIW:" > varlist
./gribpost.exe -s $input_file | grep  ":TD:"|grep -E "200 hpa|250 hpa|300 hpa|350 hpa|400 hpa|450 hpa|500 hpa|550 hpa|600 hpa|650 hpa|700 hpa|750 hpa|800 hpa|850 hpa|900 hpa|925 hpa|950 hpa|975 hpa|1000 hpa|2 m above ground" >> varlist
./gribpost.exe -s $input_file | grep  ":TADV:"|grep -E "200 hpa|500 hpa|700 hpa|850 hpa|925 hpa|1000 hpa" >> varlist
./gribpost.exe -s $input_file | grep  ":VORADV:"|grep -E "200 hpa|500 hpa|700 hpa|850 hpa|925 hpa|1000 hpa" >> varlist
./gribpost.exe -s $input_file | grep  ":TTD:"|grep -E "500 hpa|700 hpa|850 hpa|925 hpa" >> varlist
./gribpost.exe -s $input_file | grep  ":QFLX:"|grep -E "500 hpa|700 hpa|850 hpa|925 hpa" >> varlist
./gribpost.exe -s $input_file | grep  ":QDIV:"|grep -E "500 hpa|700 hpa|850 hpa|925 hpa" >> varlist
./gribpost.exe -s $input_file | grep  ":THETASE:"|grep -E "500 hpa|700 hpa|850 hpa|925 hpa" >> varlist
"""

VAR_GROUP_1 = [
    {"parameter": "HGT"},
    {"parameter": "TMP"},
    {"parameter": "UGRD"},
    {"parameter": "VGRD"},
    {"parameter": "DZDT"},
    {"parameter": "RELV"},
    {"parameter": "RELD"},
    {"parameter": "SPFH"},
    {"parameter": "RH"}
]
LEVEL_GROUP_1 = [
    {"input": 10, "output": 0.1},
    {"input": 10, "output": 0.2},
    {"input": 10, "output": 0.5},
    {"input": 10, "output": 1},
    {"input": 10, "output": 1.5},
    {"input": 10, "output": 2},
    {"input": 10, "output": 3},
    {"input": 10, "output": 4},
    {"input": 10, "output": 5},
    {"input": 10, "output": 7},
    10, 20, 30, 50, 70, 100, 150,
    200, 250, 300, 350, 400, 450, 500,
    550, 600, 650, 700, 750, 800, 850,
    900, 925, 950, 975, 1000
]

VAR_GROUP_2 = [
    {"input": {"parameter": "10u"}},
    {"input": {"parameter": "10v"}},
    {"input": {"parameter": "2t"}},
    {"input": {"parameter": "t", "level_type": "surface"}},
    {"input": {"parameter": "PRMSL"}},
    {"input": {"parameter": "PRES", "level_type": "surface"}},
    {"input": {"parameter": "2r"}},
    {
        "input": {
            "parameter": "t",
            "level_type": "depthBelowLandLayer",
            "level": {"first_level": 0, "second_level": 0.1}
        }
    },
    # {
    #     "input": {
    #         "parameter": "t",
    #         "level_type": "depthBelowLandLayer",
    #         "level": {"first_level": 0.1, "second_level": 0.3}
    #     }
    # },
    # {
    #     "input": {
    #         "parameter": "t",
    #         "level_type": "depthBelowLandLayer",
    #         "level": {"first_level": 0.3, "second_level": 0.6}
    #     }
    # },
    # {
    #     "input": {
    #         "parameter": "t",
    #         "level_type": "depthBelowLandLayer",
    #         "level": {"first_level": 0.6, "second_level": 1}
    #     }
    # },
    {
        "input": {
            "parameter": "SPFH",
            "level_type": "depthBelowLandLayer",
            "level": {"first_level": 0, "second_level": 0.1}
        }
    },
    # {
    #     "input": {
    #         "parameter": "SPFH",
    #         "level_type": "depthBelowLandLayer",
    #         "level": {"first_level": 0.1, "second_level": 0.3}
    #     }
    # },
    # {
    #     "input": {
    #         "parameter": "SPFH",
    #         "level_type": "depthBelowLandLayer",
    #         "level": {"first_level": 0.3, "second_level": 0.6}
    #     }
    # },
    # {
    #     "input": {
    #         "parameter": "SPFH",
    #         "level_type": "depthBelowLandLayer",
    #         "level": {"first_level": 0.6, "second_level": 1}
    #     }
    # },
    {"input": {"parameter": "ACPCP"}},
    {"input": {"parameter": "NCPCP"}},
    {"input": {"parameter": "APCP"}},
    # {"input": {"parameter": "LCDC"}},
    # {"input": {"parameter": "MCDC"}},
    # {"input": {"parameter": "HCDC"}},
    # {"input": {"parameter": "TCDC"}},
    {"input": {"parameter": "2t"}},
    {"input": {"parameter": "2t"}},
    {"input": {"parameter": "LHTFL"}},
    {"input": {"parameter": "LHTFL"}},
    {"input": {"parameter": "NSWRF"}},
    {"input": {"parameter": "NLWRF"}},
    {"input": {"parameter": "SNOD"}},
    {"input": {"parameter": "SNOD"}},
    {"input": {"parameter": "ALBDO"}},
    {"input": {"parameter": "SNOD"}},
    {"input": {"parameter": "SNOD"}},
    {"input": {"parameter": "HGT", "level_type": "surface"}},
    {"input": {"parameter": "HPBL", "level_type": "surface"}}
]

VAR_GROUP_3 = [
    {"parameter": "DPT"},
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 225,
        }
    },
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 224,
        }
    },
]
LEVEL_GROUP_3 = [
    200, 250, 300, 350, 400, 450, 500,
    550, 600, 650, 700, 750, 800, 850,
    900, 925, 950, 975, 1000
]

VAR_GROUP_4 = [
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 0,
            "parameterNumber": 224,
        }
    },
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 2,
            "parameterNumber": 224,
        }
    },
]
LEVEL_GROUP_4 = [
    200, 500, 700, 850, 925, 1000
]
VAR_GROUP_5 = [
    {"parameter": "DEPR"},
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 224,
        }
    },
    {
        "parameter": {
            "discipline": 0,
            "parameterCategory": 1,
            "parameterNumber": 225,
        }
    },
    {"parameter": "EPOT"}
]
LEVEL_GROUP_5 = [
    500, 700, 850, 925
]
VAR_GROUP_6 = [
    {"input": {"parameter": "2t"}}
]


def get_parameters() -> List:
    parameters = []
    for variable in VAR_GROUP_1:
        for level in LEVEL_GROUP_1:
            if isinstance(level, Dict):
                param = {
                    "input": {
                        **variable,
                        "level": level["input"],
                        "level_type": "pl"
                    },
                    "process": [
                        {
                            "type": "set",
                            "keys": {
                                "scaledValueOfFirstFixedSurface": level["output"]
                            }
                        }
                    ]
                }
            else:
                param = {
                    "input": {
                        **variable,
                        "level": level,
                        "level_type": "pl"
                    }
                }
            parameters.append(param)

    parameters.extend(VAR_GROUP_2)

    for variable in VAR_GROUP_3:
        for level in LEVEL_GROUP_3:
            parameters.append({
                "input": {
                    **variable,
                    "level": level,
                    "level_type": "pl"
                }
            })

    for variable in VAR_GROUP_4:
        for level in LEVEL_GROUP_4:
            parameters.append({
                "input": {
                    **variable,
                    "level": level,
                    "level_type": "pl"
                }
            })

    for variable in VAR_GROUP_5:
        for level in LEVEL_GROUP_5:
            parameters.append({
                "input": {
                    **variable,
                    "level": level,
                    "level_type": "pl"
                }
            })

    parameters.extend(VAR_GROUP_6)

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
        latitude=np.arange(70, -15 - 0.1, -0.1),
        longitude=np.arange(45, 145, 0.1),
        bounds_error=False,
        fill_value=missing_value,
    )

    if "process" in record:
        process_record = record["process"]
        for process_item in process_record:
            process_type = process_item["type"]
            if process_type == "set":
                keys = process_item["keys"]
                for key, value in keys.items():
                    eccodes.codes_set(m, key, value)
            else:
                print(f"process is not supported for record: {process_item} --- {record}")

    message_bytes = eccodes.codes_get_message(m)
    eccodes.codes_release(m)
    return message_bytes
