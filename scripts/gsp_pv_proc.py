"""
Download and format PV generation data for GSPs (Grid Supply Points) using the PVLive API.

This script retrieves historical PV generation data, installed capacity, and capacity for
all GSPs in Great Britain. The data is stored in an xarray Dataset with dimensions of time
and GSP ID.

The resulting dataset includes the following variables:
- generation_mw: PV generation in megawatts
- capacity_mwp: PV capacity in megawatt-peak
- installedcapacity_mwp: Installed PV capacity in megawatt-peak

Each variable is indexed by datetime_gmt and gsp_id.
"""

import xarray as xr
from pvlive_api import PVLive
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
from tqdm import tqdm

target_times = pd.date_range("2018-01-01 00:00", "2024-07-05 23:30", freq="30T")
gsp_ids = np.arange(0, 318)

x = xr.DataArray(
    np.zeros((len(target_times), len(gsp_ids))),
    coords={
        "datetime_gmt": target_times,
        "gsp_id": gsp_ids,
    },
)

ds_gsp = xr.Dataset(dict(generation_mw=x, capacity_mwp=xr.zeros_like(x), installedcapacity_mwp=xr.zeros_like(x)))

pvl = PVLive()

start = pd.Timestamp(ds_gsp.datetime_gmt.min().item()).tz_localize(timezone.utc)
end = pd.Timestamp(ds_gsp.datetime_gmt.max().item()).tz_localize(timezone.utc)


cap_dfs = []
for i in tqdm(gsp_ids):
    df = pvl.between(
        start=start,
        end=end,
        entity_type="gsp",
        entity_id=i,
        extra_fields="installedcapacity_mwp,capacity_mwp",
        dataframe=True,
    )
    df["datetime_gmt"] = df["datetime_gmt"].dt.tz_localize(None)
    df = df.sort_values("datetime_gmt").set_index("datetime_gmt")

    if start.tz_localize(None) not in df.index:
        df.loc[start.tz_localize(None)] = df.loc[start.tz_localize(None) + timedelta(minutes=30)]
    df = df.sort_index()

    ds_gsp.installedcapacity_mwp.sel(gsp_id=i).values[:] = df.installedcapacity_mwp.values
    ds_gsp.capacity_mwp.sel(gsp_id=i).values[:] = df.capacity_mwp.values
    ds_gsp.generation_mw.sel(gsp_id=i).values[:] = df.generation_mw.values


ds_gsp.to_zarr("/mnt/disks/gcp_data/pv_gsp/pvlive_gsp.zarr", consolidated=True)
