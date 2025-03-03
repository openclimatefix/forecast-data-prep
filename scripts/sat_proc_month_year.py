"""
Process and reformat satellite data from EUMETSAT SEVIRI RSS.

This script downloads satellite data from Google Cloud Storage, processes it,
and saves it in a more accessible format. It handles both HRV (High Resolution Visible)
and non-HRV data. The file names from the bucket should be confirmed
to ensure the correct data is being downloaded.

The processed data is saved in zarr format.
"""

import xarray as xr
import os
import matplotlib.pyplot as plt
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from ocf_datapipes.utils.geospatial import lon_lat_to_geostationary_area_coords
import ocf_blosc2
import numpy as np
import json

xr.set_options(keep_attrs=True)

nonhrv = True

file_end = "nonhrv.zarr" if nonhrv else "hrv.zarr"

input_root = "gs://solar-pv-nowcasting-data/satellite/EUMETSAT/SEVIRI_RSS/v4/{year}-{month}_" + file_end

output_root = "/mnt/disks/uk-all-inputs-v3/sat/v2/{year}-{month}_" + file_end

# UK and NL Crop
lon_min, lon_max = -16, 10
lat_min, lat_max = 45, 70

def rechunk(ds):
    """Rechunk the satellite data"""
    for v in ds.variables:
        del ds[v].encoding["chunks"]

    target_chunks_dict = dict(time=1, x_geostationary=100, y_geostationary=100, variable=-1)
    ds = ds.chunk(target_chunks_dict)
    return ds


if __name__ == "__main__":
    # client = Client()

    # Load old attributes
    with open("../data/old_attrs.json") as f:
        load_old_attrs = json.load(f)

    years = [2023,2024]
    months = [f"{m:02d}" for m in range(1, 13)]

    not_done = []

    for year in years:
        for month in months:

            if os.path.exists(output_root.format(year=year, month=month)):
                print(f"Skipping {year}-{month} as output file already exists.")
                continue

            print(f"Processing {year}-{month}...")

            try:
                ds = xr.open_zarr(input_root.format(year=year, month=month)).sortby("time")

                # Update attributes to match old ones, specially to update 
                ds.data.attrs.update(load_old_attrs)

                # Convert lat-lon bounds to geostationary
                ((x_min, x_max), (y_min, y_max)) = lon_lat_to_geostationary_area_coords(
                    [lon_min, lon_max],
                    [lat_min, lat_max],
                    ds.data,
                )

                # Define area - remember x is decreasing
                area = dict(x_geostationary=slice(x_max, x_min), y_geostationary=slice(y_min, y_max))

                # Slice and rechunk
                ds = rechunk(ds.sel(**area))

                with ProgressBar(dt=5):
                    ds.to_zarr(output_root.format(year=year, month=month))
                
            except FileNotFoundError:
                print(f"Input file for {year}-{month} does not exist, skipping...")
                not_done.append(f"{year}-{month}")
                continue

    # client.close()
    
    if not_done:
        print("\nThe following year-months were not processed:")
        for ym in not_done:
            print(f"- {ym}")
