from pathlib import Path

import numpy as np
import xarray as xr
import pandas as pd
from loguru import logger


PARAMETER_LIST = [
    ("u10", {"parameter": "10u"}),
    ("v10", {"parameter": "10v"}),
    ("gust10", {"parameter": "GUST"}),
    ("t2", {"parameter": "2t"}),
    ("rh2", {"parameter": "2r"}),
    ("vis", {"parameter": "VIS"}),
]

RAIN_PARAMETER = (
    "tp", {"parameter": "APCP"}
)

THRESHOLDS = [
    ("ws", 5),
    ("ws", 11),
    ("ws", 14),
    ("gs", 17),
    ("gs", 20),
    ("t", 5),
    ("r", 65),
    ("tp", 1),
    ("wis", 200),
]

"""
1 116.14444 39.91056 Shougang1 A1105 90.0
"""

LOCATION = dict(
    longitude=116.14444,
    latitude=39.91056,
    name="Shougang1",
    id="A1105",
    height=90,
)


def main():
    logger.info("program begin")

    output_directory = "/g11/wangdp/project/work/data/playground/winter/eps_station_fdp/output"
    output_file_name = "station.csv"

    file_path = Path(output_directory, output_file_name)
    df = pd.read_csv(file_path)

    forecast_list = df["forecast_time"].unique()

    forecast_data = []

    for index, forecast_hour in enumerate(forecast_list):
        record = {
            "index": index,
            "forecast_hour": forecast_hour
        }
        for name, threshold in THRESHOLDS:
            if name == "ws":
                u = df.query(f'forecast_time == {forecast_hour} & name == "u10"')["value"]
                v = df.query(f'forecast_time == {forecast_hour} & name == "v10"')["value"]
                ws = np.sqrt(u**2 + v**2)
                f = sum(ws > threshold)/len(ws) * 100
                record[f"{name.upper()}{threshold}"] = f"{f:.2f}"
            elif name == "gs":
                gs = df.query(f'forecast_time == {forecast_hour} & name == "gust10"')["value"]
                f = sum(gs > threshold) / len(gs) * 100
                record[f"{name.upper()}{threshold}"] = f"{f:.2f}"
            elif name == "t":
                t = df.query(f'forecast_time == {forecast_hour} & name == "t2"')["value"] - 273.15
                f = sum(t > threshold) / len(t) * 100
                record[f"{name.upper()}{threshold}"] = f"{f:.2f}"
            elif name == "r":
                r = df.query(f'forecast_time == {forecast_hour} & name == "rh2"')["value"]
                f = sum(r > threshold) / len(r) * 100
                record[f"{name.upper()}{threshold}"] = f"{f:.2f}"
            elif name == "tp":
                tp = df.query(f'forecast_time == {forecast_hour} & name == "tp"')["value"]
                f = sum(tp > threshold) / len(tp) * 100
                record[f"{name.upper()}{threshold}"] = f"{f:.2f}"
            elif name == "wis":
                wis = df.query(f'forecast_time == {forecast_hour} & name == "vis"')["value"] / 1000
                f = sum(wis > threshold) / len(wis) * 100
                record[f"{name.upper()}{threshold}"] = f"{f:.2f}"
            else:
                raise ValueError(f"name is not supported: {name}")
        forecast_data.append(record)

    prod_df = pd.DataFrame(forecast_data)
    print(prod_df)

    logger.info("program end")


if __name__ == "__main__":
    main()
