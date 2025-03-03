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

xr.set_options(keep_attrs=True)

nonhrv = True

file_end = "nonhrv.zarr" if nonhrv else "hrv.zarr"

input_root = "gs://public-datasets-eumetsat-solar-forecasting/satellite/EUMETSAT/SEVIRI_RSS/v4/" "{year}_" + file_end

output_root = "/mnt/disks/uk-all-inputs-v3/sat/v2/test/{year}_" + file_end

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

    for year in [2019]:
        if os.path.exists(output_root.format(year=year)):
            continue

        print(year)

        ds = xr.open_zarr(input_root.format(year=year)).sortby("time")

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
            ds.to_zarr(output_root.format(year=year))

    # client.close()
