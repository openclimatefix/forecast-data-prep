"""
Uploads ECMWF Zarr folders from a specified directory to a Google Cloud Storage (GCS) bucket.

Args:
----
    directory (str): The path to the directory containing the Zarr folders to upload.
    bucket_name (str): The name of the GCS bucket to upload the Zarr folders to.
"""

import multiprocessing
import os
from subprocess import CalledProcessError, call

from tqdm import tqdm


def upload_zarr_folder(folder_path, bucket_name):
    try:
        call(["gsutil", "-m", "cp", "-r", folder_path, f"gs://{bucket_name}/"])
    except CalledProcessError as e:
        print(f"Failed to upload {folder_path}: {e}")


def get_all_zarr_folders(directory):
    zarr_folders = []
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isdir(item_path) and item_path.endswith(".zarr"):
            zarr_folders.append(item_path)
    return zarr_folders


def upload_zarr_folder_with_progress(folder_path, bucket_name, queue):
    upload_zarr_folder(folder_path, bucket_name)
    queue.put(1)


def main(directory, bucket_name):
    zarr_folders_to_upload = get_all_zarr_folders(directory)

    # Create a manager to track progress
    manager = multiprocessing.Manager()
    queue = manager.Queue()

    # Initialize the pool with 20 processes
    pool = multiprocessing.Pool(processes=20)

    # Add progress tracking with tqdm
    progress_bar = tqdm(total=len(zarr_folders_to_upload), desc="Uploading Zarr Folders")

    # Launch the upload tasks
    for folder_path in zarr_folders_to_upload:
        pool.apply_async(upload_zarr_folder_with_progress, args=(folder_path, bucket_name, queue))

    # Close the pool
    pool.close()

    # Update the progress bar
    for _ in range(len(zarr_folders_to_upload)):
        queue.get()
        progress_bar.update(1)

    # Ensure all processes have completed
    pool.join()

    progress_bar.close()


if __name__ == "__main__":
    # Change this to your directory path
    directory = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/uk_ext/t2"
    bucket_name = "solar-pv-nowcasting-data/NWP/ECMWF/UK_extended/"  # Change this to your GCS bucket name
    main(directory, bucket_name)
