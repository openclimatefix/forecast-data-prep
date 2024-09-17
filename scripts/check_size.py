"""
This script checks the size of Zarr directories and saves the file size information to a CSV file.
"""

import os
import glob
import pandas as pd
from dask.distributed import Client, LocalCluster, progress


def get_zarr_size(file_path):
    """Get the size of a Zarr directory."""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(file_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return file_path, total_size / (1024**2)  # Return size in MB


def main():
    # Directory containing the Zarr datasets
    tmp_dir = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/UK_Met_Office/UKV_ext/tmp_2"

    # Gather all Zarr files
    file_list = glob.glob(f"{tmp_dir}/*.zarr")
    print(f"Found {len(file_list)} files to check.")

    # Set up Dask client with at least 10 workers
    cluster = LocalCluster(n_workers=10)
    client = Client(cluster)

    # Distribute the task of calculating file sizes
    futures = client.map(get_zarr_size, file_list)

    # Use Dask's progress bar to display progress
    progress(futures)

    # Gather the results
    results = client.gather(futures)

    # Convert results to DataFrame
    df = pd.DataFrame(results, columns=["file_path", "size_MB"])

    # Save the file size information to a CSV file
    output_csv_path = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/UK_Met_Office/UKV_ext/zarr_file_sizes.csv"
    df.to_csv(output_csv_path, index=False)

    print(f"File size information saved to {output_csv_path}")


if __name__ == "__main__":
    main()
