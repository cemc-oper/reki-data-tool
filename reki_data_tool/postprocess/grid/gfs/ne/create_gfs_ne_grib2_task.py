from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

from reki_data_tool.postprocess.grid.gfs.ne.config import (
    get_random_start_time,
    get_random_forecast_time,
    OUTPUT_DIRECTORY
)


def create_serial_task(output_script_path: Path):
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


def create_dask_v1_task(output_script_path: Path):
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
        partition="normal",
        nodes=1,
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


if __name__ == "__main__":
    output_script_dir = "/g11/wangdp/project/work/data/playground/operation/gfs/ne/output"
    output_script_path = Path(output_script_dir, "gfs_ne_grib2_dask_v1_case_1.sh")

    create_dask_v1_task(output_script_path)
