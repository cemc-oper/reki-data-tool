import xarray as xr
from tqdm.auto import tqdm


def load_all_dataset(
        data_path_root,
        parameter_list,
        show_progress=True
):
    dss = []
    if show_progress:
        ps = tqdm(parameter_list)
    else:
        ps = parameter_list
    for parameter in ps:
        name = parameter.get("name", parameter["parameter"])
        ds = xr.open_mfdataset(f"{data_path_root}/*.{name}.nc")
        dss.append(ds)

    ds = xr.merge(dss)
    return ds
