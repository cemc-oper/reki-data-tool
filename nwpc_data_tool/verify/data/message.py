from nwpc_data.grib.eccodes import load_message_from_file


def get_message(
        data_path,
        parameter,
        level,
        level_type="isobaricInhPa",
):
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
