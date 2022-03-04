from typing import Union, Tuple, Dict, List, Callable

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


def cal_run_time(func):
    def wrapper(*args, **kwargs):
        start_time = pd.Timestamp.now()
        func(*args, **kwargs)
        end_time = pd.Timestamp.now()
        print(end_time - start_time)
    return wrapper


def create_dask_client(engine: str = "local", client_kwargs: Dict = None) -> Client:
    """
    Create dask client with scheduler and workers.

    Parameters
    ----------
    engine
        local or mpi (require dask_mpi)
    client_kwargs

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
            interface="ib0",
            dashboard=False,
            nthreads=1
        )
        client = Client(**client_kwargs)
    else:
        raise ValueError(f"engine is not supported: {engine}")

    return client


def get_message_count(file_path):
    with open(file_path, "rb") as f:
        total_count = eccodes.codes_count_in_file(f)
        logger.info(f"total count: {total_count}")
    return total_count