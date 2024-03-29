from typing import Union, Tuple, Dict, List, Callable
import os

import eccodes
import xarray as xr
import pandas as pd

from dask.distributed import Client
from loguru import logger


def extract_level(
        field: xr.DataArray,
        levels: List
) -> xr.DataArray:
    """
    抽取层次
    """
    return field.sel(
        pl=levels
    )


def extract_domain(
        field: xr.DataArray,
        lat_index: Union[int, Tuple[int, int]],
        lon_index: Union[int, Tuple[int, int]],
) -> xr.DataArray:
    """
    截取范围

    Parameters
    ----------
    field
    lat_index
    lon_index

    Returns
    -------

    """
    latitude = lat_index
    if isinstance(lat_index, Tuple):
        latitude = slice(*lat_index)
    longitude = lon_index
    if isinstance(lon_index, Tuple):
        longitude = slice(*lon_index)

    domain_field = field.isel(
        latitude=latitude,
        longitude=longitude,
    )

    return domain_field


def combine_fields(
        fields: List[xr.DataArray],
        field_record: Dict,
        dim: str="valid_time",
) -> xr.DataArray:
    """
    按维度 step 合并
    """
    field = xr.concat(fields, dim=dim)
    return field


def compute_field(
        op: Callable,
        *args,
) -> xr.DataArray:
    """
    计算要素场
    """
    return op(*args)


def cal_run_time(func) -> Callable:
    """
    A function wrapper to print function's run time.

    Parameters
    ----------
    func

    Returns
    -------
    Callable
    """
    def wrapper(*args, **kwargs):
        start_time = pd.Timestamp.now()
        func(*args, **kwargs)
        end_time = pd.Timestamp.now()
        print("[cal_run_time] function run time:", end_time - start_time)
    return wrapper


def create_dask_client(
        engine: str = "local",
        client_kwargs: Dict = None
) -> Client:
    """
    Create dask client with scheduler and workers.

    Parameters
    ----------
    engine
        how to create dask workers:

        * local
        * mpi (require dask_mpi)
    client_kwargs
        used as args when create ``dask.distributed``
    Returns
    -------
    Client
    """
    if client_kwargs is None:
        client_kwargs = dict()

    if engine == "local":
        client = Client(**client_kwargs)
    elif engine == "mpi":
        from dask_mpi import initialize
        initialize(
            # nanny=False,
            # protocol='tcp',
            interface="ib0",
            dashboard=False,
            nthreads=1,
            # local_directory=os.getcwd()
            local_directory=os.getenv('TMPDIR'),  # use ${TMPDIR} in HPC2023
            # exit=False,
        )
        client = Client(**client_kwargs)

        # use in SLURM.
        # We expect SLURM_NTASKS workers
        N = int(os.getenv('SLURM_NTASKS')) - 2

        logger.info("wait for workers and report...")
        client.wait_for_workers(n_workers=N)
        logger.info("wait for workers and report...done")

        num_workers = len(client.scheduler_info()['workers'])
        logger.info("%d workers available and ready" % num_workers)
        # print(client.scheduler_info())

    else:
        raise ValueError(f"engine is not supported: {engine}")

    return client


def get_message_count(file_path) -> int:
    """
    Get total number of GRIB messages in one GRIB file.

    Parameters
    ----------
    file_path

    Returns
    -------
    int
    """
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    return total_count
