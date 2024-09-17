"""
This script is used to process zipped zarr files from a specified directory.
The script processes the data in specific way for Met Office data, then chunks the data
and saves the init_time processed files in a directory.
"""

import os
from dask.diagnostics import ProgressBar
import dask
import xarray as xr
import glob
from dask.distributed import Client


input_files = sorted(glob.glob("/mnt/storage_b/nwp/ceda/uk/*.zarr.zip"))
tmp_dir = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/UK_Met_Office/UKV_ext/tmp_2"

os.makedirs(tmp_dir, exist_ok=False)

target_chunks_dict = dict(init_time=1, x=100, y=100, step=10, variable=-1)

tmp_files = []


@dask.delayed
def download_zipped_zarr(path):
    filename = path.split("/")[-1]

    ds = xr.open_zarr(path).compute()

    ds = ds.to_array(dim="variable").to_dataset(name="UKV")

    ds["variable"] = ds.variable.astype(str)

    ds = ds.chunk(target_chunks_dict)
    new_filename = filename.removesuffix(".zip")

    ds.to_zarr(f"{tmp_dir}/{new_filename}")


if __name__ == "__main__":
    client = Client(processes=False, n_workers=1, threads_per_worker=10)

    print("Saving files")
    tasks = [download_zipped_zarr(f) for f in input_files]
    print(len(tasks))
    with ProgressBar(dt=1):
        dask.compute(tasks, scheduler=client)

    print("Completed processing zarr files for Met Office UKV")
