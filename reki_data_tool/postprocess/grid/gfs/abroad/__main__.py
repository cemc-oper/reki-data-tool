from pathlib import Path
from typing import Optional

import typer

from reki.data_finder import find_local_file
from reki_data_tool.postprocess.grid.gfs.util import parse_time_options

app = typer.Typer()


@app.command()
def serial(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        output_file_path: Optional[Path] = typer.Option(None)
):
    from reki_data_tool.postprocess.grid.gfs.abroad.task_serial import make_abroad_data_serial

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

    make_abroad_data_serial(
        input_file_path=input_file_path,
        output_file_path=output_file_path,
    )


@app.command()
def dask_v1(
        start_time: Optional[str] = None,
        forecast_time: Optional[str] = None,
        input_file_path: Optional[Path] = None,
        output_file_path: Optional[Path] = typer.Option(...),
        engine: str = "local",
):
    from reki_data_tool.postprocess.grid.gfs.abroad.task_dask_v1 import make_abroad_data_by_dask_v1

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

    make_abroad_data_by_dask_v1(
        input_file_path=input_file_path,
        output_file_path=output_file_path,
        engine=engine
    )


if __name__ == "__main__":
    app()
