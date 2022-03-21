from pathlib import Path
from typing import Optional

import pandas as pd
import typer

from reki.data_finder import find_local_file
from reki_data_tool.postprocess.station.winter.meso1km.utils import get_random_start_time


app = typer.Typer()


@app.command()
def serial(
        station_id: str = typer.Option(...),
        start_time: Optional[str] = typer.Option(None),
        forecast_length: int = typer.Option(24),
        use_postvar: bool = False,
        output_file_path: Optional[Path] = typer.Option(None),
):
    if start_time is None:
        start_time = get_random_start_time()
    else:
        start_time = pd.to_datetime(start_time, format="%Y%m%d%H")

    postvar_file = None
    if use_postvar:
        typer.echo("Don't support postvar")
        raise typer.Exit()

    grib2_files = []
    for forecast_hour in range(0, forecast_length + 1):
        grib2_data_path = find_local_file(
            "cma_meso_1km/grib2/orig",
            start_time=start_time,
            forecast_time=pd.Timedelta(hours=forecast_hour),
            data_class="smart2022",
        )
        if grib2_data_path is None:
            typer.echo(f"file path not found: {start_time}")
            raise typer.Exit()
        grib2_files.append(grib2_data_path)

    from reki_data_tool.postprocess.station.winter.meso1km.task_serial import create_station_serial
    create_station_serial(
        output_file=output_file_path,
        station_id=station_id,
        grib2_files=grib2_files,
        postvar_file=postvar_file,
    )


@app.command()
def dask_v1(
        station_id: str = typer.Option(...),
        start_time: Optional[str] = typer.Option(None),
        forecast_length: int = typer.Option(24),
        use_postvar: bool = False,
        output_file_path: Optional[Path] = typer.Option(None),
):
    pass

if __name__ == "__main__":
    app()
