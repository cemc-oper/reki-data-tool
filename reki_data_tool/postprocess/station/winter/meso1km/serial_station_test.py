import pathlib

import xarray as xr
import pandas as pd
import numpy as np
from loguru import logger
import click

from reki.format.grib.eccodes import load_field_from_file as load_grib2_field_from_file
from reki.format.grads import load_field_from_file as load_grads_field_from_file

from reki_data_tool.postprocess.station import (
    extract_domain,
    combine_fields,
    compute_field,
)

from reki_data_tool.postprocess.station.winter import (
    standard_station,
    standard_lat_section,
    standard_lon_section,
)

from reki_data_tool.postprocess.station.winter.meso1km.common import (
    LEVELS,
    NAMES,
    DATASET_NAMES,
)


@click.command("generate-station")
@click.option("--output-file", help="output file path", required=True)
def generate_station_in_serial(output_file):
    logger.info("program begin")

    # 站点信息
    station_lat_index = 405
    station_lon_index = 797

    # 剖面图范围
    lat_index_range = (180, 861)
    lon_index_range = (520, 1221)

    logger.info("getting file list...")

    grib2_data_path = pathlib.Path(
        "/g11/wangdp/project/work/data/playground/station/ncl/data",
        "grib2-orig"
    )
    grib2_files = list(grib2_data_path.glob("rmf.hgra.*.grb2"))
    grib2_files = sorted(grib2_files)

    postvar_file_path = pathlib.Path(
        "/g11/wangdp/project/work/data/playground/station/ncl/data",
        "postvar/postvar.ctl_202108251200000"
    )

    logger.info("loading fields from files...")
    data_list = dict()
    for field_record in NAMES:
        field_name = field_record["field_name"]
        data_source = field_record.get("data_source", "grib2")

        stations = []
        lat_sections = []
        lon_sections = []

        if data_source == "grib2":
            for file_path in grib2_files[:2]:
                field = load_grib2_field_from_file(
                    file_path,
                    parameter=field_name,
                    level_type="pl",
                    level=LEVELS,
                )
                # level_field = extract_level(field, levels)
                field_station = extract_domain(field, station_lat_index, station_lon_index)
                field_lat_section = extract_domain(field, lat_index_range, station_lon_index)
                field_lon_section = extract_domain(field, station_lat_index, lon_index_range)
                print(field_lat_section.latitude.values.shape)
                stations.append(field_station)
                lat_sections.append(field_lat_section)
                lon_sections.append(field_lon_section)
        elif data_source == "postvar":
            for forecast_hour in pd.to_timedelta(np.arange(0, 2, 1), unit="h"):
                field = load_grads_field_from_file(
                    postvar_file_path,
                    parameter=field_name,
                    level_type="pl",
                    forecast_time=forecast_hour,
                    level=LEVELS
                )
                if field is None:
                    raise ValueError("field not found!")

                # level_field = extract_level(field, levels)
                field_station = extract_domain(field, station_lat_index, station_lon_index)
                field_lat_section = extract_domain(field, lat_index_range, station_lon_index)
                field_lon_section = extract_domain(field, station_lat_index, lon_index_range)
                print(field_lat_section.latitude.values.shape)
                stations.append(field_station)
                lat_sections.append(field_lat_section)
                lon_sections.append(field_lon_section)
        else:
            raise ValueError(f"data source is not supported: {data_source}")

        data_list[f"{field_name}"] = combine_fields(stations, field_record, dim="valid_time")
        data_list[f"{field_name}_0"] = combine_fields(lat_sections, field_record, dim="valid_time")
        data_list[f"{field_name}_9"] = combine_fields(lon_sections, field_record, dim="valid_time")
    logger.info("loading fields from files...done")

    logger.info("generating dataset fields...")
    dataset_list = dict()
    for record in DATASET_NAMES:
        name = record["name"]
        if "fields" not in record:
            field_name = record["field_name"]
            current_station = data_list[f"{field_name}"]
            current_lat_section = data_list[f"{field_name}_0"]
            current_lon_section = data_list[f"{field_name}_9"]
        else:
            op = record["operator"]
            current_station = compute_field(op, *[data_list[f"{f['field_name']}"] for f in record["fields"]])
            current_lat_section = compute_field(op, *[data_list[f"{f['field_name']}_0"] for f in record["fields"]])
            current_lon_section = compute_field(op, *[data_list[f"{f['field_name']}_9"] for f in record["fields"]])

        dataset_list[f"{name}"] = standard_station(current_station, record)
        dataset_list[f"{name}_0"] = standard_lat_section(current_lat_section, record)
        dataset_list[f"{name}_9"] = standard_lon_section(current_lon_section, record)
    logger.info("generating dataset fields...done")

    logger.info("creating xarray.Dataset...")
    ds = xr.Dataset(dataset_list)
    logger.info("creating xarray.Dataset...done")

    logger.info("saving to NetCDF file...")
    ds.to_netcdf(output_file, format="NETCDF3_CLASSIC")
    logger.info(f"saving to NetCDF file...done, {output_file}")

    logger.info("program end")


if __name__ == "__main__":
    generate_station_in_serial()
