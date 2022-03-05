from pathlib import Path
from typing import Optional

import typer
import pandas as pd

from reki_data_tool.postprocess.grid.gfs.util import get_random_start_time, get_random_forecast_time

app = typer.Typer()


@app.command()
def serial(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        output_file_path: Optional[Path] = typer.Option(None)
):
    from reki_data_tool.postprocess.grid.gfs.ne.task_serial import create_grib2_ne

    start_time, forecast_time = parse_time_options(start_time, forecast_time)

    create_grib2_ne(
        start_time=start_time,
        forecast_time=forecast_time,
        output_file_path=output_file_path,
    )


@app.command()
def dask_v1(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        output_file_path: Optional[Path] = typer.Option(None),
        engine: str = "local",
):
    from reki_data_tool.postprocess.grid.gfs.ne.task_dask_v1 import create_grib2_ne_dask_v1

    start_time, forecast_time = parse_time_options(start_time, forecast_time)

    create_grib2_ne_dask_v1(
        start_time=start_time,
        forecast_time=forecast_time,
        output_file_path=output_file_path,
        engine=engine
    )


@app.command()
def dask_v2(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        output_file_path: Optional[Path] = typer.Option(None),
        engine: str = typer.Option("local"),
        batch_size: int = typer.Option(32),
):
    from reki_data_tool.postprocess.grid.gfs.ne.task_dask_v2 import create_grib2_ne_dask_v2

    start_time, forecast_time = parse_time_options(start_time, forecast_time)

    create_grib2_ne_dask_v2(
        start_time=start_time,
        forecast_time=forecast_time,
        output_file_path=output_file_path,
        engine=engine,
        batch_size=batch_size,
    )


def parse_time_options(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None
) -> (pd.Timestamp, pd.Timedelta):
    if start_time is None:
        start_time = get_random_start_time()
    else:
        start_time = pd.to_datetime(start_time, format="%Y%m%d%H")

    if forecast_time is None:
        forecast_time = get_random_forecast_time()
    else:
        forecast_time = pd.to_timedelta(forecast_time)
    return start_time, forecast_time


if __name__ == "__main__":
    app()
