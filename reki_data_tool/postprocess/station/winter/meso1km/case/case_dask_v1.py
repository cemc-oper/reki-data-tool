from pathlib import Path
from dataclasses import dataclass

from typer.testing import CliRunner
from loguru import logger

from reki_data_tool.postprocess.station.winter.meso1km.create_task import app


CASE_BASE_DIRECTORY = "/g11/wangdp/project/work/data/playground/operation/winter/meso1km/station/case/11-dask-v1"


runner = CliRunner()


def test_dask_v1():
    station_id = "54406"
    nodes_list = (1, 2, 4, 8)
    count = 20
    partition = "normal"

    script_base_directory = Path(CASE_BASE_DIRECTORY, "script")

    for node_count in nodes_list:
        for test_index in range(1, count+1):
            logger.info(f"create job script for NODE {node_count} TEST {test_index}...")
            script_path = Path(script_base_directory, f"node_{node_count:02}", f"test_{test_index:02}.cmd")
            script_path.parent.mkdir(parents=True, exist_ok=True)

            work_dir = Path(CASE_BASE_DIRECTORY, f"node_{node_count:02}", f"test_{test_index:02}")
            work_dir.mkdir(parents=True, exist_ok=True)
            result = runner.invoke(app, [
                "dask-v1",
                "--station-id", station_id,
                "--output-script-path", script_path.absolute(),
                "--work-directory", work_dir.absolute(),
                "--nodes", node_count,
                "--partition", partition,
            ])


def test_dask_v1_less_than_one_node():
    @dataclass
    class TestCase:
        nodes: int
        tasks_per_node: int

    test_cases = [
        TestCase(1, 2 + 1),
        TestCase(1, 2 + 2),
        TestCase(1, 2 + 4),
        TestCase(1, 2 + 8),
        TestCase(1, 2 + 16),
        TestCase(1, 2 + 30),
    ]

    count = 20
    partition = "normal"

    script_base_directory = Path(CASE_BASE_DIRECTORY, "script")

    for test_case in test_cases:
        nodes = test_case.nodes
        tasks_per_node = test_case.tasks_per_node
        total_task = tasks_per_node * nodes - 2

        for test_index in range(1, count+1):
            logger.info(f"create job script for TASKS {tasks_per_node} TEST {test_index}...")
            script_path = Path(script_base_directory, f"task_n{nodes:02}t{total_task:02}", f"test_{test_index:02}.cmd")
            script_path.parent.mkdir(parents=True, exist_ok=True)

            work_dir = Path(CASE_BASE_DIRECTORY, f"task_n{nodes:02}t{total_task:02}", f"test_{test_index:02}")
            work_dir.mkdir(parents=True, exist_ok=True)
            result = runner.invoke(app, [
                "dask-v1",
                "--output-script-path", script_path.absolute(),
                "--work-directory", work_dir.absolute(),
                "--nodes", 1,
                "--ntasks-per-node", tasks_per_node,
                "--partition", partition
            ])
            print(result)


if __name__ == "__main__":
    test_dask_v1()
