import operator


# 层次条件
LEVELS = [
    100.,
    150.,
    200.,
    250.,
    300.,
    400.,
    500.,
    600.,
    700.,
    800.,
    850.,
    900.,
    925.,
    950.,
    1000
]


# 抽取变量场
NAMES = [
    {
        "field_name": "gh",
    },
    {
        "field_name": "q",
    },
    {
        "field_name": "r",
    },
    {
        "field_name": "t",
    },
    {
        "field_name": "u",
    },
    {
        "field_name": "v",
    },
    {
        # use postvar version
        # "field_name": "w",
        # "data_source": "postvar",

        # use grib2 version
        "field_name": "wz"
    },
    {
        "field_name": "snmr",
    },
    {
        "field_name": "icmr",
    },
    {
        "field_name": "rwmr",
    },
]


# 输出要素场
DATASET_NAMES = [
    {
        "field_name": "gh",
        "name": "GH",
        "long_name": "Geopotential height",
        "units": "gpm",
    },
    {
        "field_name": "q",
        "name": "Q",
        "long_name": "Specific humidity",
        "units": "g/kg",
    },
    {
        "field_name": "r",
        "name": "R",
        "long_name": "Relative humidity",
        "units": "%",
    },
    {
        "field_name": "t",
        "name": "T",
        "long_name": "Temperature",
        "units": "C",
    },
    {
        "field_name": "u",
        "name": "U",
        "long_name": "U-component of wind",
        "units": "m/s",
    },
    {
        "field_name": "v",
        "name": "V",
        "long_name": "V-component of wind",
        "units": "m/s",
    },
    {
        "field_name": "wz",
        "name": "W",
        "long_name": "Vertical velocity (geometric)",
        "units": "m/s",
    },
    {
        "name": "CIWC",
        "long_name": "Ice water and Snow mixing ratio",
        "units": "g/kg",
        "operator": operator.add,
        "fields": [
            {
                "field_name": "snmr"
            },
            {
                "field_name": "icmr"
            },
        ]
    },
    {
        "name": "CLWC",
        "long_name": "Cloud and Rain mixing ratio",
        "units": "g/kg",
        "operator": operator.add,
        "fields": [
            {
                "field_name": "snmr"
            },
            {
                "field_name": "rwmr"
            },
        ]
    }
]


STATIONS = {
    "54406": {
        "point": {
            "lat_index": 405,
            "lon_index": 797
        },
        "section": {
            "lat_index_range": (180, 861),
            "lon_index_range": (520, 1221)
        }
    }
}
