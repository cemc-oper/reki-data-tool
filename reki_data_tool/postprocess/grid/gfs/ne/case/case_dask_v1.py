from pathlib import Path

from typer.testing import CliRunner
from loguru import logger

from reki_data_tool.postprocess.grid.gfs.ne.create_task import app


CASE_BASE_DIRECTORY = "/g11/wangdp/project/work/data/playground/operation/gfs/ne/case/11-dask-v1"


runner = CliRunner()


def test_dask_v1():
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
                "--output-script-path", script_path.absolute(),
                "--work-directory", work_dir.absolute(),
                "--nodes", node_count,
                "--partition", partition
            ])


if __name__ == "__main__":
    test_dask_v1()
