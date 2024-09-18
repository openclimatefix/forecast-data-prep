"""
Download Zarr folders from a Google Cloud Storage (GCS) bucket to a local directory.

It uses multiprocessing to download multiple folders concurrently, improving efficiency.
The script lists all Zarr folders in the specified GCS bucket, creates the local directory
if it doesn't exist, and then downloads each folder using gsutil.

Progress is tracked and displayed using a progress bar.
"""

import multiprocessing
import os
from subprocess import PIPE, CalledProcessError, run

from tqdm import tqdm


def download_zarr_folder(bucket_path, local_directory):
    try:
        local_directory = os.path.abspath(local_directory)
        run(["gsutil", "-m", "cp", "-r", f"gs://{bucket_path}", local_directory], check=True)
        print(f"Successfully downloaded {bucket_path} to {local_directory}")
    except CalledProcessError as e:
        print(f"Failed to download {bucket_path}: {e}")


def get_all_zarr_folders(bucket_name):
    try:
        result = run(["gsutil", "ls", f"gs://{bucket_name}"], check=True, stdout=PIPE, stderr=PIPE, text=True)
        if result.returncode != 0:
            print(f"gsutil ls failed with error: {result.stderr}")
            return []
        folders = [line.strip().split("gs://")[-1] for line in result.stdout.splitlines() if line.endswith("/")]
        print(f"Found folders: {folders}")
        return folders
    except CalledProcessError as e:
        print(f"Failed to list folders in {bucket_name}: {e}")
        return []


def download_zarr_folder_with_progress(bucket_path, local_directory, queue):
    download_zarr_folder(bucket_path, local_directory)
    queue.put(1)


def main(bucket_name, local_directory):
    local_directory = os.path.abspath(local_directory)
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)
        print(f"Created local directory: {local_directory}")

    zarr_folders_to_download = get_all_zarr_folders(bucket_name)

    if not zarr_folders_to_download:
        print("No folders to download.")
        return

    # Create a manager to track progress
    manager = multiprocessing.Manager()
    queue = manager.Queue()

    pool = multiprocessing.Pool(processes=100)

    # Add progress tracking with tqdm
    progress_bar = tqdm(total=len(zarr_folders_to_download), desc="Downloading Zarr Folders")

    # Launch the download tasks
    for folder_path in zarr_folders_to_download:
        pool.apply_async(download_zarr_folder_with_progress, args=(folder_path, local_directory, queue))

    # Close the pool
    pool.close()

    # Update the progress bar
    try:
        for _ in range(len(zarr_folders_to_download)):
            queue.get()
            progress_bar.update(1)
    except KeyboardInterrupt:
        print("Process interrupted. Terminating pool...")
        pool.terminate()
    finally:
        # Ensure all processes have completed
        pool.join()

    progress_bar.close()


if __name__ == "__main__":
    # Change this to your GCS bucket name
    bucket_name = "solar-pv-nowcasting-data/NWP/UK_Met_Office/UKV_extended/"
    # Change this to your local directory path
    local_directory = "/mnt/disks/gcp_data/nwp/ukv/ukv_ext"
    main(bucket_name, local_directory)
