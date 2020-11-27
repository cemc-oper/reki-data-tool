import typing

import xarray as xr


def set_attrs_for_station(
        field: xr.DataArray,
        field_record: typing.Dict
) -> xr.DataArray:
    """
    设置站点垂直剖面数据属性

    Parameters
    ----------
    field
    field_record

    Returns
    -------

    """

    if "lon_0" in field.attrs:
        lon_0 = field.attrs["lon_0"]
    else:
        lon_0 = field.longitude.item()

    if "lat_0" in field.attrs:
        lat_0 = field.attrs["lat_0"]
    else:
        lat_0 = field.latitude.item()

    field.attrs = {
        "name": field_record["name"],
        "long_name": field_record["long_name"],
        "units": field_record["units"],
        "lat_0": lon_0,
        "lon_0": lat_0,
    }
    return field


def change_coords_for_station(
        field: xr.DataArray
) -> xr.DataArray:
    """
    修改站点垂直剖面图坐标

    Parameters
    ----------
    field

    Returns
    -------

    """
    return field.reset_coords(drop=True).rename({
        "pl": "level",
        "valid_time": "time",
    })


def set_attrs_for_lat_section(
        field: xr.DataArray,
        field_record: typing.Dict
) -> xr.DataArray:
    if "lon_0" in field.attrs:
        lon_0 = field.attrs["lon_0"]
    else:
        lon_0 = field.longitude.item()
    field.attrs = {
        "name": field_record["name"],
        "long_name": field_record["long_name"],
        "units": field_record["units"],
        "lon_0": lon_0,
    }
    return field


def change_coords_for_lat_section(
        field: xr.DataArray
) -> xr.DataArray:
    return field.reset_coords(drop=True).rename({
        "pl": "level",
        "valid_time": "time",
        "latitude": "lat_0"
    })[:,:,::-1]


def set_attrs_for_lon_section(
        field: xr.DataArray,
        field_record: typing.Dict
) -> xr.DataArray:
    if "lat_0" in field.attrs:
        lat_0 = field.attrs["lat_0"]
    else:
        lat_0 = field.latitude.item()
    field.attrs = {
        "name": field_record["name"],
        "long_name": field_record["long_name"],
        "units": field_record["units"],
        "lat_0": lat_0,
    }
    return field


def change_coords_for_lon_section(
        field: xr.DataArray,
) -> xr.DataArray:
    return field.reset_coords(drop=True).rename({
        "pl": "level",
        "valid_time": "time",
        "longitude": "lon_9"
    })
