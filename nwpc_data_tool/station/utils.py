import typing

import xarray as xr


def extract_level(
        field: xr.DataArray,
        levels: typing.List
) -> xr.DataArray:
    """
    抽取层次
    """
    return field.sel(
        pl=levels
    )

def extract_domain(
        field: xr.DataArray,
        lat_index: typing.Union[int, typing.Tuple[int, int]],
        lon_index: typing.Union[int, typing.Tuple[int, int]],
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
    if isinstance(lat_index, typing.Tuple):
        latitude = slice(*lat_index)
    longitude = lon_index
    if isinstance(lon_index, typing.Tuple):
        longitude = slice(*lon_index)

    domain_field = field.isel(
        latitude=latitude,
        longitude=longitude,
    )

    return domain_field

def combine_fields(
        fields: xr.DataArray,
        field_record: typing.Dict,
        dim: str="valid_time",
) -> xr.DataArray:
    """
    按维度 step 合并
    """
    field = xr.concat(fields, dim=dim)
    return field

def compute_field(
        op: typing.Callable,
        *args,
) -> xr.DataArray:
    """
    计算要素场
    """
    return op(*args)