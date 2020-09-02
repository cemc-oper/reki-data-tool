import numpy as np
import xarray as xr

from nwpc_data.grib.eccodes import load_field_from_file


def get_field(
        data_path,
        parameter,
        level,
        level_type="isobaricInhPa",
) -> xr.DataArray:
    if parameter == "wind":
        field = get_wind_field(
            data_path=data_path,
            level=level,
            level_type=level_type
        )
    else:
        field = load_field_from_file(
            file_path=data_path,
            parameter=parameter,
            level_type=level_type,
            level=level
        )
    return field


def get_wind_field(
        data_path,
        level,
        parameter=("u", "v"),
        level_type="isobaricInhPa",
) -> xr.DataArray:
    u_field = load_field_from_file(
        file_path=data_path,
        parameter=parameter[0],
        level_type=level_type,
        level=level
    )
    v_field = load_field_from_file(
        file_path=data_path,
        parameter=parameter[1],
        level_type=level_type,
        level=level
    )

    def magnitude(a, b):
        func = lambda x, y: np.sqrt(x ** 2 + y ** 2)
        return xr.apply_ufunc(func, a, b)
    field = magnitude(u_field, v_field)
    return field
