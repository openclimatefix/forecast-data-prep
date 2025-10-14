"""
Process Met Office UKV forecast data by filtering by year and rechunking for optimal performance.

The input and output directories can be set within the main function along with the years to process.
The script reads from a single zarr store and outputs yearly optimised zarr v3 files with efficient
chunk sizes for machine learning workloads.

Output: Yearly optimised zarr v3 files containing UKV forecast data

14/10/25
IMPORTANT: The crop size in this script is set up to keep just the southern half of the UK.
"""


import xarray as xr


def rechunk_ukv(input_zarr, output_dir, year):
    ds = xr.open_zarr(input_zarr)

    # filter for a current year
    ds = ds.sel(init_time=str(year))

    # crop for UK
    print("cropping dataset")

    # crop data for the uk
    # This crop size is set up to keep just the southern half of the UK
    ds = ds.sel(x_osgb=slice(100000, 720000), y_osgb=slice(500000, -100000))

    print("chunking dataset")

    # Optimise chunk sizing
    ds = ds.chunk({"init_time": 1, "step": 12, "variable": 14, "y_osgb": 20, "x_osgb": 20})

    # Drop encoding is needed if using zarr v3, which I am to save this file.
    print(f"Saving dataset for year {year}")

    ds.drop_encoding().to_zarr(f"{output_dir}ukv_v8_{year}.zarr", mode="w")
    print(f"Saved year {year}")

    return


def main():
    years = range(2017, 2026)

    for year in years:
        print(f"Processing year {year}")

        rechunk_ukv(
            input_zarr="/mnt/leonardo/archives/nwp/mo-um-ukv/data/mo-um-ukv.zarr",
            output_dir="/mnt/storage_u2_30tb_a/ml_training_zarrs/nwp/ukv_v8_south_uk_crop/",
            year=year,
        )


if __name__ == "__main__":
    main()
