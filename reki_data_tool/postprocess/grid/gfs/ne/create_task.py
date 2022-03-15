from pathlib import Path

import pandas as pd
import typer
from jinja2 import Environment, FileSystemLoader

from reki.data_finder import find_local_file
from reki_data_tool.postprocess.grid.gfs.ne.config import OUTPUT_DIRECTORY
from reki_data_tool.postprocess.grid.gfs.util import get_random_start_time, get_random_forecast_time


app = typer.Typer()


@app.command("serial")
def create_serial_task(
        output_script_path: Path = typer.Option(Path(OUTPUT_DIRECTORY, "gfs_ne_grib2_serial_case_1.sh"))
):
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{int(forecast_time / pd.Timedelta(hours=1)):03}"
    print(start_time_label, forecast_time_label)

    output_directory = OUTPUT_DIRECTORY
    output_file_path = Path(
        output_directory,
        f'ne_{start_time_label}_{forecast_time_label}.grb2'
    )

    file_loader = FileSystemLoader(Path(__file__).parent)
    env = Environment(loader=file_loader)

    template = env.get_template("slurm_job.sh")

    job_params = dict(
        job_name=output_script_path.stem,
        is_parallel=False,
        partition="serial",
        model_path="reki_data_tool.postprocess.grid.gfs.ne",
        options=f"""serial \\
            --start-time={start_time_label} \\
            --forecast-time={forecast_time_label}h \\
            --output-file-path={output_file_path}"""
    )

    task_script_content = template.render(**job_params)
    with open(output_script_path, "w") as f:
        f.write(task_script_content)

    return output_script_path


@app.command("dask-v1")
def create_dask_v1_task(
        output_script_path: Path = typer.Option(Path(OUTPUT_DIRECTORY, "dask_v1_case_1.sh")),
        nodes: int = 1,
        partition: str = "normal"
):
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{int(forecast_time / pd.Timedelta(hours=1)):03}"
    print(start_time_label, forecast_time_label)

    output_directory = OUTPUT_DIRECTORY
    output_file_path = Path(
        output_directory,
        f'ne_{start_time_label}_{forecast_time_label}.grb2'
    )

    file_loader = FileSystemLoader(Path(__file__).parent)
    env = Environment(loader=file_loader)

    template = env.get_template("slurm_job.sh")

    job_params = dict(
        job_name=output_script_path.stem,
        is_parallel=True,
        partition=partition,
        nodes=nodes,
        ntasks_per_node=32,
        model_path="reki_data_tool.postprocess.grid.gfs.ne",
        options=f"""dask-v1 \\
            --start-time={start_time_label} \\
            --forecast-time={forecast_time_label}h \\
            --output-file-path={output_file_path} \\
            --engine=mpi"""
    )

    task_script_content = template.render(**job_params)
    with open(output_script_path, "w") as f:
        f.write(task_script_content)

    return output_script_path


@app.command("dask-v2")
def create_dask_v1_task(
        output_script_path: Path = typer.Option(Path(OUTPUT_DIRECTORY, "dask_v2_case_1.sh")),
        nodes: int = 4,
        partition: str = "normal"
):
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{int(forecast_time / pd.Timedelta(hours=1)):03}"
    print(start_time_label, forecast_time_label)

    output_directory = OUTPUT_DIRECTORY
    output_file_path = Path(
        output_directory,
        f'ne_{start_time_label}_{forecast_time_label}.grb2'
    )

    file_loader = FileSystemLoader(Path(__file__).parent)
    env = Environment(loader=file_loader)

    template = env.get_template("slurm_job.sh")

    job_params = dict(
        job_name=output_script_path.stem,
        is_parallel=True,
        partition=partition,
        nodes=nodes,
        ntasks_per_node=32,
        model_path="reki_data_tool.postprocess.grid.gfs.ne",
        options=f"""dask-v2 \\
            --start-time={start_time_label} \\
            --forecast-time={forecast_time_label}h \\
            --output-file-path={output_file_path} \\
            --engine=mpi \\
            --batch-size={nodes*32}"""
    )

    task_script_content = template.render(**job_params)
    with open(output_script_path, "w") as f:
        f.write(task_script_content)

    return output_script_path


@app.command("gribpost")
def create_gribpost_task(
        output_script_path: Path = typer.Option(Path(OUTPUT_DIRECTORY, "01-gribpost", "cmd_tool_gribpost_v1.sh")),
        partition: str = "serial"
):
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{int(forecast_time / pd.Timedelta(hours=1)):03}"
    print(start_time_label, forecast_time_label)

    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )

    output_directory = Path(OUTPUT_DIRECTORY, "01-gribpost")

    file_loader = FileSystemLoader(Path(__file__).parent)
    env = Environment(loader=file_loader)

    template = env.get_template("01_gribpost.sh")

    job_params = dict(
        partition=partition,
        job_name=output_script_path.stem,
        run_dir=output_directory.absolute(),
        origin_file_path=file_path.absolute(),
        origin_file_name=file_path.name,
        target_file_name=f'ne_{start_time_label}_{forecast_time_label}.grb2',
    )

    task_script_content = template.render(**job_params)
    with open(output_script_path, "w") as f:
        f.write(task_script_content)

    return output_script_path


@app.command("wgrib2")
def create_gribpost_task(
        output_script_path: Path = typer.Option(Path(OUTPUT_DIRECTORY, "02-wgrib2", "cmd_tool_wgrib2_v1.sh")),
        partition: str = "serial"
):
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    forecast_time = get_random_forecast_time()
    forecast_time_label = f"{int(forecast_time / pd.Timedelta(hours=1)):03}"
    print(start_time_label, forecast_time_label)

    file_path = find_local_file(
        "grapes_gfs_gmf/grib2/orig",
        start_time=start_time,
        forecast_time=forecast_time
    )

    output_directory = Path(OUTPUT_DIRECTORY, "02-wgrib2")

    file_loader = FileSystemLoader(Path(__file__).parent)
    env = Environment(loader=file_loader)

    template = env.get_template("02_wgrib2.sh")

    job_params = dict(
        partition=partition,
        job_name=output_script_path.stem,
        run_dir=output_directory.absolute(),
        origin_file_path=file_path.absolute(),
        origin_file_name=file_path.name,
        target_file_name=f'ne_{start_time_label}_{forecast_time_label}.grb2',
    )

    task_script_content = template.render(**job_params)
    with open(output_script_path, "w") as f:
        f.write(task_script_content)

    return output_script_path


if __name__ == "__main__":
    app()
