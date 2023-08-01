from pathlib import Path
from typing import Union

from loguru import logger

from reki_data_tool.utils import cal_run_time, create_dask_client
from reki_data_tool.postprocess.grid.gfs.abroad.common import get_parameters, get_message_bytes


@cal_run_time
def make_abroad_data_by_dask_v1(
        input_file_path: Union[Path, str],
        output_file_path: Union[Path, str],
        engine: str = "local",
):
    # close Heartbeat check, see the following page:
    #
    #   https://dask.discourse.group/t/dask-workers-killed-because-of-heartbeat-fail/856/3
    from dask import config as cfg
    cfg.set({'distributed.scheduler.worker-ttl': None})

    logger.info("program begin")
    parameters = get_parameters()

    if engine == "local":
        client_kwargs = dict(threads_per_worker=1)
    else:
        client_kwargs = dict()
    client = create_dask_client(engine, client_kwargs=client_kwargs)
    print(client)

    bytes_futures = []
    for record in parameters:
        f = client.submit(get_message_bytes, input_file_path, record)
        bytes_futures.append(f)

    # def get_object(l):
    #     return l
    #
    # bytes_lists = dask.delayed(get_object)(bytes_futures)
    # f = bytes_lists.persist()
    # progress(f)
    # bytes_futures = f.compute()

    total_count = len(parameters)
    with open(output_file_path, "wb") as f:
        for i, fut in enumerate(bytes_futures):
            message_bytes = client.gather(fut)
            del fut
            logger.info(f"writing message...{i + 1}/{total_count}")
            if message_bytes is not None:
                f.write(message_bytes)
                del message_bytes

    client.close()

    logger.info("program done")
