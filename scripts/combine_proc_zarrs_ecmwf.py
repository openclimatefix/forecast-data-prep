"""
This script is designed to process zarr NWPs from ECMWF that require combination along the "Init_time" dimension.

Functions:
    - main: Use the processed unzipped NWP zarrs and combine along init time.

Output:
    - Combined zarr files for each year.
"""

import os
from dask.diagnostics import ProgressBar
import xarray as xr
import glob
from dask.distributed import Client, LocalCluster
import numpy as np
import datetime

output_file = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/uk_ext/t2/ECMWF_{year}.zarr"
tmp_dir = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/uk_ext/tmp"

assert not os.path.exists(output_file)

if __name__ == "__main__":
    cluster = LocalCluster(
        n_workers=30,  # Adjust based on your system's capability
        threads_per_worker=2,
        memory_limit="6GB",
    )

    client = Client(cluster)

    print("Loading dataset from files")

    # Find all unique years based on the filenames in the tmp_dir
    file_list = glob.glob(f"{tmp_dir}/*.zarr")
    year_list = [int(f.split("/")[-1][:4]) for f in file_list]
    unique_years = np.unique(year_list)

    print(f"All unique years {unique_years}")

    # sometimes, certain years can fail, to avoid re-running this for all years
    # a list of years can be specified below instead such as:
    # set_years = [2021, 2022, 2023]

    for year in unique_years:  # Change to set_years to process only specific years
        assert not os.path.exists(output_file.format(year=year))

        current_time = datetime.datetime.now()
        print(f"Processing year: {year} - Current time: {current_time}")
        ds_all = xr.open_mfdataset(
            f"{tmp_dir}/{year}*.zarr", engine="zarr", data_vars="all"
        )
        ds_all = ds_all.sortby("init_time")
        ds_all["variable"] = ds_all.variable.astype(str)

        print(f"Saving {year} dataset to combined zarr")
        with ProgressBar(dt=10):
            ds_all.to_zarr(output_file.format(year=year))
