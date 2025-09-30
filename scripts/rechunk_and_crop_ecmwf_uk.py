"""
Process ECMWF forecast data by combining zarr files, filtering by year, rechunking for performance,
convert data to float32 and cropping to the UK region.

The input and ouput dir can be set within the main funciton as well as the years to iterate over.
Also allows the ability to exclude specific zarr files from processing.

Output: Yearly optimised Zarrs (zarr v3)
"""

import glob

import xarray as xr


def rechunk_and_crop_ecmwf(input_dir, output_dir, year):
    # The following files have corupted data
    exclude_files = ["2024040100-2024043012.zarr", "2022060100-2022063012.zarr"]

    paths = [f for f in glob.glob(input_dir + "*.zarr") if f.split("/")[-1] not in exclude_files]

    ds = xr.open_mfdataset(paths, chunks="auto", decode_timedelta=True)

    # filter for a current year
    ds = ds.sel(init_time=str(year))

    for var in ds.data_vars:
        ds[var].encoding.pop("chunks", None)

    print("chunking dataset")

    # Optimise chunk sizing
    ds = ds.chunk({"variable": 18, "init_time": 1, "step": 12, "latitude": 10, "longitude": 10})

    print("coverting to float32")

    # Convert coordinates and data variables from float64 to float32
    ds["latitude"] = ds.latitude.astype("float32")
    ds["longitude"] = ds.longitude.astype("float32")
    ds["hres-ifs_west-europe"] = ds["hres-ifs_west-europe"].astype("float32")

    ds["variable"] = ds.variable.astype("object")  # did this to get around an error when saving to zarr

    print("cropping dataset")

    # crop data for the uk
    ds = ds.sel(longitude=slice(-12, 3), latitude=slice(62, 48))

    # Drop encoding is needed if using zarr v3, which I am to save this file.
    print(f"Saving dataset for year {year}")

    ds.drop_encoding().to_zarr(f"{output_dir}ecmwf_uk_crop_{year}.zarr", mode="w")

    print(f"Saved year {year}")

    return


def main():
    years = range(2019, 2026)

    for year in years:
        print(f"Processing year {year}")

        rechunk_and_crop_ecmwf(
            input_dir="/mnt/leonardo/nwp/ecmwf-hres-ifs-west-europe/data/",
            output_dir="/mnt/storage_u2_4tb_a/ecmwf_uk_v4_crop_rechunked/",
            year=year,
        )


if __name__ == "__main__":
    main()
