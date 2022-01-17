import pathlib

import xarray as xr
import numpy as np
import pandas as pd
import click

import dask
from dask.distributed import Client, progress
from dask_mpi import initialize

from reki.format.grib.eccodes import load_field_from_file as load_grib2_field_from_file
from reki.format.grads import load_field_from_file as load_grads_field_from_file

from reki_data_tool.station.utils import (
    extract_domain,
    combine_fields,
    compute_field,
)

from reki_data_tool.station.winter.utils import (
    standard_station,
    standard_lat_section,
    standard_lon_section,
)

from reki_data_tool.station.winter.condition import (
    levels,
    names,
    dataset_names,
)

import logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s][%(name)s][%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


@click.command("generate-station")
@click.option("--output-file", help="output file path", required=True)
@click.option("--threads-per-worker", default=1, help="thread count per worker")
def generate_station_in_dask(output_file, threads_per_worker):
    logger.info("initializing mpi...")
    initialize(
        interface="ib0",
        nthreads=threads_per_worker,
        dashboard=False,
    )

    logger.info("program begin")

    start_time = "2021082512"

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

    logger.debug("start client...")
    client = Client()
    print(client)

    logger.info("loading fields from files...")
    data_list = dict()
    for field_record in names:
        data_source = field_record.get("data_source", "grib2")
        field_name = field_record["field_name"]
        stations = []
        lat_sections = []
        lon_sections = []

        if data_source == "grib2":
            for file_path in grib2_files:
                field = dask.delayed(load_grib2_field_from_file)(
                    file_path,
                    parameter=field_name,
                    level_type="pl",
                    level=levels
                )
                # level_field = dask.delayed(extract_level)(field, levels)
                field_station = dask.delayed(extract_domain)(field, station_lat_index, station_lon_index)
                field_lat_section = dask.delayed(extract_domain)(field, lat_index_range, station_lon_index)
                field_lon_section = dask.delayed(extract_domain)(field, station_lat_index, lon_index_range)
                stations.append(field_station)
                lat_sections.append(field_lat_section)
                lon_sections.append(field_lon_section)
        elif data_source == "postvar":
            for forecast_hour in pd.to_timedelta(np.arange(0, 25, 1), unit="h"):
                field = dask.delayed(load_grads_field_from_file)(
                    postvar_file_path,
                    parameter=field_name,
                    level_type="pl",
                    forecast_time=forecast_hour,
                    level=levels
                )
                if field is None:
                    raise ValueError("field not found!")

                # level_field = extract_level(field, levels)
                field_station = dask.delayed(extract_domain)(field, station_lat_index, station_lon_index)
                field_lat_section = dask.delayed(extract_domain)(field, lat_index_range, station_lon_index)
                field_lon_section = dask.delayed(extract_domain)(field, station_lat_index, lon_index_range)
                stations.append(field_station)
                lat_sections.append(field_lat_section)
                lon_sections.append(field_lon_section)
        else:
            raise ValueError(f"data source is not supported: {data_source}")

        data_list[f"{field_name}_0"] = dask.delayed(combine_fields)(lat_sections, field_record, dim="valid_time")
        data_list[f"{field_name}_9"] = dask.delayed(combine_fields)(lon_sections, field_record, dim="valid_time")
        data_list[f"{field_name}"] = dask.delayed(combine_fields)(stations, field_record, dim="valid_time")
    logger.info("loading fields from files...done")

    logger.info("generating dataset fields...")
    dataset_list = dict()
    for record in dataset_names:
        name = record["name"]
        if "fields" not in record:
            field_name = record["field_name"]
            current_station = data_list[f"{field_name}"]
            current_lat_section = data_list[f"{field_name}_0"]
            current_lon_section = data_list[f"{field_name}_9"]
        else:
            op = record["operator"]
            current_station = dask.delayed(compute_field)(op,
                                                          *[data_list[f"{f['field_name']}"] for f in record["fields"]])
            current_lat_section = dask.delayed(compute_field)(op, *[data_list[f"{f['field_name']}_0"] for f in
                                                                    record["fields"]])
            current_lon_section = dask.delayed(compute_field)(op, *[data_list[f"{f['field_name']}_9"] for f in
                                                                    record["fields"]])

        dataset_list[f"{name}_0"] = dask.delayed(standard_lat_section)(current_lat_section, record)
        dataset_list[f"{name}_9"] = dask.delayed(standard_lon_section)(current_lon_section, record)
        dataset_list[f"{name}"] = dask.delayed(standard_station)(current_station, record)
    logger.info("generating dataset fields...done")

    def get_data_list(dataset_list):
        return dataset_list

    t = dask.delayed(get_data_list)(dataset_list)

    logger.info("run DAG...")
    result = t.persist()
    progress(result)

    r = result.compute()
    logger.info("run DAG...done")

    client.close()

    logger.info("creating xarray.Dataset...")
    ds = xr.Dataset(r)

    # 维度属性和变量
    ds.coords["level"].attrs = {
        "long_name": "Isobaric surface",
        "units": "hPa"
    }
    ds["level"] = ds.coords["level"]

    # 数据集属性
    ds.attrs = {
        "model": "GRAPES-1KM",
        "initial_time": f"{start_time}0000"
    }

    logger.info("creating xarray.Dataset...done")

    logger.info("saving to NetCDF file...")
    ds.to_netcdf(output_file, format="NETCDF3_CLASSIC")
    logger.info(f"saving to NetCDF file...done, {output_file}")

    logger.info("program end")


if __name__ == "__main__":
    generate_station_in_dask()
