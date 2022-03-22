from pathlib import Path

import typer
from jinja2 import Environment, FileSystemLoader

from reki_data_tool.postprocess.station.winter.meso1km.utils import get_random_start_time
from reki_data_tool.postprocess.station.winter.meso1km.config import OUTPUT_DIRECTORY


app = typer.Typer()


@app.command("serial")
def create_serial_task(
        output_script_path: Path = typer.Option(Path(OUTPUT_DIRECTORY, "03-serial", "station_03_serial_case_1.sh")),
        work_directory: Path = typer.Option(Path(OUTPUT_DIRECTORY)),
):
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    print(start_time_label)

    output_directory = work_directory
    output_file_path = Path(
        output_directory,
        f'station_{start_time_label}.grb2'
    )

    file_loader = FileSystemLoader(Path(__file__).parent)
    env = Environment(loader=file_loader)

    template = env.get_template("slurm_job.sh")

    job_params = dict(
        job_name=output_script_path.stem,
        is_parallel=False,
        partition="serial",
        model_path="reki_data_tool.postprocess.station.winter.meso1km",
        options=f"""serial \\
            --station-id=54406 \\
            --start-time={start_time_label} \\
            --output-file-path={output_file_path}"""
    )

    task_script_content = template.render(**job_params)
    with open(output_script_path, "w") as f:
        f.write(task_script_content)

    return output_script_path


@app.command("dask-v1")
def create_dask_v1_task(
        station_id: str = "54406",
        output_script_path: Path = typer.Option(Path(OUTPUT_DIRECTORY, "11-dask-v1", "station_11_dask_v1_case_1.sh")),
        work_directory: Path = typer.Option(Path(OUTPUT_DIRECTORY)),
        nodes: int = 1,
        partition: str = typer.Option("normal"),
        engine: str = typer.Option("mpi")
):
    start_time = get_random_start_time()
    start_time_label = start_time.strftime("%Y%m%d%H")
    print(start_time_label)

    output_directory = work_directory
    output_file_path = Path(
        output_directory,
        f'station_{start_time_label}.nc'
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
        model_path="reki_data_tool.postprocess.station.winter.meso1km",
        options=f"""dask-v1 \\
            --station-id={station_id} \\
            --start-time={start_time_label} \\
            --output-file-path={output_file_path} \\
            --engine={engine}"""
    )

    task_script_content = template.render(**job_params)
    with open(output_script_path, "w") as f:
        f.write(task_script_content)

    return output_script_path


if __name__ == "__main__":
    app()
