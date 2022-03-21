from pathlib import Path
from typing import List, Union

import xarray as xr
import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from loguru import logger

from reki.format.grib.eccodes import load_field_from_file as load_grib2_field_from_file
from reki.format.grads import load_field_from_file as load_grads_field_from_file

from reki_data_tool.utils import (
    extract_domain,
    combine_fields,
    compute_field,
)

from reki_data_tool.postprocess.station.winter.meso1km.utils import (
    standard_station,
    standard_lat_section,
    standard_lon_section,
)

from reki_data_tool.postprocess.station.winter.meso1km.common import (
    LEVELS,
    NAMES,
    DATASET_NAMES,
    STATIONS
)

from reki_data_tool.utils import cal_run_time


@cal_run_time
def create_station_serial(
        output_file: Union[str, Path],
        station_id: str,
        grib2_files: List[Union[str, Path]],
        postvar_file: Union[str, Path] = None,
):
    logger.info("program begin")

    # 站点信息
    station_lat_index = STATIONS[station_id]["point"]["lat_index"]
    station_lon_index = STATIONS[station_id]["point"]["lon_index"]

    # 剖面图范围
    lat_index_range = STATIONS[station_id]["section"]["lat_index_range"]
    lon_index_range = STATIONS[station_id]["section"]["lon_index_range"]

    logger.info("getting file list...")

    logger.info("loading fields from files...")
    data_list = dict()
    for field_record in tqdm(NAMES):
        field_name = field_record["field_name"]
        data_source = field_record.get("data_source", "grib2")

        stations = []
        lat_sections = []
        lon_sections = []

        if data_source == "grib2":
            for file_path in tqdm(grib2_files):
                field = load_grib2_field_from_file(
                    file_path,
                    parameter=field_name,
                    level_type="pl",
                    level=LEVELS,
                )
                if field is None:
                    raise ValueError("field not found!")

                # level_field = extract_level(field, levels)
                field_station = extract_domain(field, station_lat_index, station_lon_index)
                field_lat_section = extract_domain(field, lat_index_range, station_lon_index)
                field_lon_section = extract_domain(field, station_lat_index, lon_index_range)
                stations.append(field_station)
                lat_sections.append(field_lat_section)
                lon_sections.append(field_lon_section)
        elif data_source == "postvar":
            if postvar_file is None:
                raise ValueError("postvar_file must be set for postvar data source.")
            for forecast_hour in pd.to_timedelta(np.arange(0, 25, 1), unit="h"):
                field = load_grads_field_from_file(
                    postvar_file,
                    parameter=field_name,
                    level_type="pl",
                    forecast_time=forecast_hour,
                    level=LEVELS
                )
                if field is None:
                    raise ValueError("field not found!")

                # level_field = extract_level(field, LEVELS)
                field_station = extract_domain(field, station_lat_index, station_lon_index)
                field_lat_section = extract_domain(field, lat_index_range, station_lon_index)
                field_lon_section = extract_domain(field, station_lat_index, lon_index_range)
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
    grib2_data_path = Path(
        # "/g11/wangdp/project/work/data/playground/station/ncl/data",
        # "grib2-orig"
        "/g2/nwp_sp/OPER_ARCHIVE/GRAPES_MESO_1KM/Prod-grib/2022031300/ORIG"
    )
    grib2_files = list(grib2_data_path.glob("rmf.hgra.*.grb2"))
    grib2_files = sorted(grib2_files)

    postvar_file_path = Path(
        "/g11/wangdp/project/work/data/playground/station/ncl/data",
        "postvar/postvar.ctl_202108251200000"
    )

    station_id = "54406"

    from reki_data_tool.postprocess.station.winter.meso1km.config import (
        OUTPUT_DIRECTORY
    )
    output_file_path = Path(OUTPUT_DIRECTORY, f"station_{station_id}_03_serial.nc")

    create_station_serial(
        output_file=output_file_path,
        station_id=station_id,
        grib2_files=grib2_files,
        postvar_file=None,
    )
