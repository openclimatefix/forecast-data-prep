"""
This script is for processing the extended horizon ECMWF dataset, which goes out to 83 hours and was gathered by OCF during April-July 2024.
The script processes the data in specific way for ECMWF data, then chunks the data and saves the init_time processed files in a directory.
"""

import os
from dask.diagnostics import ProgressBar
import dask
import xarray as xr
import glob
from dask.distributed import Client

input_files = sorted(glob.glob("/mnt/storage_b/nwp/ecmwf/uk/*.zarr.zip"))
tmp_dir = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/uk_ext/tmp"


os.makedirs(tmp_dir, exist_ok=False)

target_chunks_dict = dict(init_time=1, longitude=60, latitude=50, step=10, variable=-1)

tmp_files = []


@dask.delayed
def download_zipped_zarr(path):
    filename = path.split("/")[-1]

    ds = xr.open_zarr(path).compute()

    ds = ds.rename(
        {
            "ssrd": "dswrf",
            "strd": "dlwrf",
            "dsrp": "sr",
            "uvb": "duvrs",
        }
    )

    ds = ds.to_array(dim="variable").to_dataset(name="ECMWF_UK")

    ds["variable"] = ds.variable.astype(str)

    # Reorder the variables for more specific chunking for PVNet
    ds = ds.sel(
        variable=[
            "t2m",
            "dswrf",
            "dlwrf",
            "sr",
            "duvrs",
            "hcc",
            "lcc",
            "mcc",
            "tcc",
            "sd",
            "u10",
            "u100",
            "v10",
            "v100",
        ]
    )

    ds = ds.chunk(target_chunks_dict)
    new_filename = filename.removesuffix(".zip")

    ds.to_zarr(f"{tmp_dir}/{new_filename}")


if __name__ == "__main__":
    client = Client(processes=False, n_workers=1, threads_per_worker=5)

    print("Saving chunked files")
    tasks = [download_zipped_zarr(f) for f in input_files]
    print(len(tasks))
    with ProgressBar(dt=1):
        dask.compute(tasks, scheduler=client)

    print("Completed processing zarr files for ECMWF")
