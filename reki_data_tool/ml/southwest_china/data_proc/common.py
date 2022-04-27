from typing import List, Optional

import eccodes
import numpy as np

from reki.format.grib.eccodes import load_message_from_file
from reki.format.grib.eccodes.operator import interpolate_grid


VAR_GROUP_1 = [
    {"parameter": "t"},                                 # 温度
    {"parameter": "u"},                                 # U 风
    {"parameter": "v"},                                 # V 风
    {"parameter": "wz"},                                # W 风
    {"parameter": "gh"},                               # 高度
]
LEVEL_GROUP_1 = [
    975, 950, 925, 850, 700, 500, 250
]

VAR_GROUP_1_MESO = [
    {"parameter": "t"},                                 # 温度
    {"parameter": "u"},                                 # U 风
    {"parameter": "v"},                                 # V 风
    {"parameter": "gh"},                                # 高度
    {"parameter": "q"},                                 # 比湿
    {"parameter": "clwmr"},                             # 云水
    {"parameter": "icmr"},                              # 冰水
    {"parameter": "rwmr"},                              # 雨水
    {"parameter": "snmr"},                              # 雪水
    {"parameter": "grle"},                              # 霰

]
LEVEL_GROUP_1_MESO = [
    975, 950, 925, 850, 700, 500, 200
]

VAR_GROUP_2 = [
    {"parameter": "2t"},                                # 2米温度
    {"parameter": "sp"},                                # 地面气压
    {"parameter": "2r"},                               # 2米相对湿度
    {"parameter": "10u"},                               # U 风
    {"parameter": "10v"},                               # V 风
    {"parameter": "TCDC"},                              # 总云量
    {"parameter": "LCDC"},                              # 低云量
    {"parameter": "APCP"},                              # 总降水
    {"parameter": "hflux"},                             # 地表感热通量
    {"parameter": "slhf"},                              # 地表潜热通量
]

VAR_GROUP_3 = [
    {"parameter": "2t"},                                # 2米温度
    {"parameter": "sp"},                                # 地面气压
    {"parameter": "2r"},                               # 2米相对湿度
    {"parameter": "10u"},                               # U 风
    {"parameter": "10v"},                               # V 风
    # {"parameter": "TCDC"},                              # 总云量
    # {"parameter": "LCDC"},                              # 低云量
    {"parameter": "APCP"},                              # 总降水
    {"parameter": "hflux"},                             # 地表感热通量
    {"parameter": "pwat"},                              # 整层可降水量
    {"parameter": "CAPE"},                              # CAPE
    {"parameter": "CIN"},                               # CIN
    {"parameter": {"discipline": 0, "parameterCategory": 16, "parameterNumber": 224}},  # 雷达组合反射率
    {"parameter": "kx"},                                # K
]


def get_gfs_parameters() -> List:
    parameters = []
    for variable in VAR_GROUP_1:
        for level in LEVEL_GROUP_1:
            parameters.append({
                **variable,
                "level": level,
                "level_type": "pl"
            })

    parameters.extend(VAR_GROUP_2)

    return parameters


def get_meso_parameters() -> List:
    parameters = []
    for variable in VAR_GROUP_1_MESO:
        for level in LEVEL_GROUP_1_MESO:
            parameters.append({
                **variable,
                "level": level,
                "level_type": "pl"
            })

    parameters.extend(VAR_GROUP_3)

    return parameters


def get_message_bytes(file_path, record) -> Optional[bytes]:
    m = load_message_from_file(file_path, **record)
    if m is None:
        return None

    m = interpolate_grid(
        m,
        latitude=np.arange(45, -45, -0.28125),
        longitude=np.arange(0, 360, 0.28125),
        # bounds_error=False,
        # fill_value=None,
    )

    message_bytes = eccodes.codes_get_message(m)
    eccodes.codes_release(m)
    return message_bytes
