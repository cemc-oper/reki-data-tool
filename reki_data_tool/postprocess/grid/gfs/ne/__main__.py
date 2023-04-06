from pathlib import Path
from typing import Optional

import typer

from reki_data_tool.postprocess.grid.gfs.util import parse_time_options

app = typer.Typer()


@app.command()
def serial(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        longitude: str = typer.Option(None),
        latitude: str = typer.Option(None),
        output_file_path: Optional[Path] = typer.Option(None)
):
    from reki.data_finder import find_local_file
    from reki_data_tool.postprocess.grid.gfs.ne.task_serial import make_grib2_ne_serial

    if input_file_path is None:
        start_time, forecast_time = parse_time_options(start_time, forecast_time)
        input_file_path = find_local_file(
            "grapes_gfs_gmf/grib2/orig",
            start_time=start_time,
            forecast_time=forecast_time
        )

    if input_file_path is None:
        print("input file path is empty. Please check options.")
        raise typer.Exit(code=2)

    start_longitude, end_longitude, start_latitude, end_latitude = parse_grid(longitude, latitude)

    make_grib2_ne_serial(
        input_file_path=input_file_path,
        start_longitude=start_longitude,
        end_longitude=end_longitude,
        start_latitude=start_latitude,
        end_latitude=end_latitude,
        output_file_path=output_file_path,
    )


@app.command()
def dask_v1(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        longitude: str = typer.Option(None),
        latitude: str = typer.Option(None),
        output_file_path: Optional[Path] = typer.Option(None),
        engine: str = "local",
):
    from reki.data_finder import find_local_file
    from reki_data_tool.postprocess.grid.gfs.ne.task_dask_v1 import make_grib2_ne_dask_v1

    if input_file_path is None:
        start_time, forecast_time = parse_time_options(start_time, forecast_time)
        input_file_path = find_local_file(
            "grapes_gfs_gmf/grib2/orig",
            start_time=start_time,
            forecast_time=forecast_time
        )

    if input_file_path is None:
        print("input file path is empty. Please check options.")
        raise typer.Exit(code=2)

    start_longitude, end_longitude, start_latitude, end_latitude = parse_grid(longitude, latitude)

    make_grib2_ne_dask_v1(
        input_file_path=input_file_path,
        start_longitude=start_longitude,
        end_longitude=end_longitude,
        start_latitude=start_latitude,
        end_latitude=end_latitude,
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


def parse_grid(longitude: str, latitude):
    lon_tokens = longitude.split(":")
    if len(lon_tokens) != 2:
        raise ValueError("longitude must be start_longitude:end_longitude")
    start_longitude = float(lon_tokens[0])
    end_longitude = float(lon_tokens[1])

    lat_tokens = latitude.split(":")
    if len(lat_tokens) != 2:
        raise ValueError("latitude must be start_latiitude:end_latitude")
    start_latitude = float(lat_tokens[0])
    end_latitude = float(lat_tokens[1])

    return start_longitude, end_longitude, start_latitude, end_latitude




if __name__ == "__main__":
    app()
