from reki.grib.eccodes import load_message_from_file

import numpy as np
import eccodes


def get_message(
        data_path,
        parameter,
        level,
        level_type="isobaricInhPa",
):
    if parameter == "wind":
        u_message, v_message = get_wind_message(
            data_path=data_path,
            level=level,
            level_type=level_type,
        )
        message = eccodes.codes_clone(u_message)
        u_array = eccodes.codes_get_double_array(u_message, "values")
        v_array = eccodes.codes_get_double_array(v_message, "values")
        w_array = np.sqrt(u_array*u_array + v_array*v_array)
        eccodes.codes_release(u_message)
        eccodes.codes_release(v_message)
        eccodes.codes_set_double_array(message, "values", w_array)
    else:
        message = load_message_from_file(
            file_path=data_path,
            parameter=parameter,
            level_type=level_type,
            level=level
        )
    return message


def get_wind_message(
        data_path,
        level,
        parameter=("u", "v"),
        level_type="isobaricInhPa",
):
    u_message = load_message_from_file(
        file_path=data_path,
        parameter=parameter[0],
        level_type=level_type,
        level=level
    )
    v_message = load_message_from_file(
        file_path=data_path,
        parameter=parameter[1],
        level_type=level_type,
        level=level
    )

    return u_message, v_message
