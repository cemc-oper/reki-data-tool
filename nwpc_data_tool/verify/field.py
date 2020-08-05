from nwpc_data.grib.eccodes import load_field_from_file


def get_field(
        data_path,
        parameter,
        level,
        level_type="isobaricInhPa",
):
    field = load_field_from_file(
        file_path=data_path,
        parameter=parameter,
        level_type=level_type,
        level=level
    )
    return field
