import multiprocessing
import os
from subprocess import CalledProcessError, call
from tqdm import tqdm
import time


def upload_zarr_folder(folder_path, bucket_name, failed_counter, retries=3, wait_time=5):
    """
    Uploads a single Zarr folder to GCS with retries in case of failure.
    
    Args:
        folder_path (str): Path to the Zarr folder.
        bucket_name (str): The GCS bucket to upload the folder to.
        failed_counter (multiprocessing.Value): Shared counter for tracking failed uploads.
        retries (int): Number of retry attempts in case of failure.
        wait_time (int): Wait time (in seconds) between retries.
    """
    for attempt in range(retries):
        try:
            print(f"Attempting to upload {folder_path} to gs://{bucket_name}/ (Attempt {attempt + 1}/{retries})")
            call(["gsutil", "-m", "rsync", "-r", folder_path, f"gs://{bucket_name}/"])
            print(f"Successfully uploaded {folder_path} to gs://{bucket_name}/")
            return  # Exit function if successful
        except CalledProcessError as e:
            if attempt < retries - 1:
                print(f"Failed to upload {folder_path} on attempt {attempt + 1}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)  # Wait before retrying
            else:
                print(f"Failed to upload {folder_path} after {retries} attempts. Error: {e}")
                with failed_counter.get_lock():
                    failed_counter.value += 1  # Increment the failure count


def get_all_zarr_folders(directory):
    """
    Gets all Zarr folders from the specified directory.
    
    Args:
        directory (str): The directory containing Zarr folders.
        
    Returns:
        List[str]: List of Zarr folder paths.
    """
    print(f"Scanning directory {directory} for Zarr folders...")
    zarr_folders = [os.path.join(directory, item) for item in os.listdir(directory) 
                    if os.path.isdir(os.path.join(directory, item)) and item.endswith(".zarr")]
    
    print(f"Found {len(zarr_folders)} Zarr folders to upload.")
    return zarr_folders


def upload_zarr_folder_with_progress(folder_path, bucket_name, queue, failed_counter):
    """
    Uploads a Zarr folder and updates the progress queue.
    
    Args:
        folder_path (str): Path to the Zarr folder.
        bucket_name (str): GCS bucket name.
        queue (multiprocessing.Queue): Queue to update progress.
        failed_counter (multiprocessing.Value): Shared counter for tracking failed uploads.
    """
    print(f"Starting upload for {folder_path}...")
    upload_zarr_folder(folder_path, bucket_name, failed_counter)
    queue.put(1)
    print(f"Finished upload for {folder_path}.")


def main(directory, bucket_name):
    """
    Main function to upload all Zarr folders in a directory to GCS.
    
    Args:
        directory (str): Directory containing the Zarr folders.
        bucket_name (str): GCS bucket name.
    """
    print(f"Starting the upload process for directory {directory} to bucket {bucket_name}...")
    zarr_folders_to_upload = get_all_zarr_folders(directory)

    # Create a manager to track progress
    manager = multiprocessing.Manager()
    queue = manager.Queue()

    # Initialize the pool with 20 processes
    pool = multiprocessing.Pool(processes=20)

    # Create a shared counter for failed uploads
    failed_counter = multiprocessing.Value('i', 0)

    # Add progress tracking with tqdm
    progress_bar = tqdm(total=len(zarr_folders_to_upload), desc="Uploading Zarr Folders")

    # Launch the upload tasks
    for folder_path in zarr_folders_to_upload:
        print(f"Queueing upload task for {folder_path}...")
        pool.apply_async(upload_zarr_folder_with_progress, args=(folder_path, bucket_name, queue, failed_counter))

    # Close the pool
    pool.close()

    # Update the progress bar
    for _ in range(len(zarr_folders_to_upload)):
        queue.get()
        progress_bar.update(1)

    # Ensure all processes have completed
    pool.join()

    progress_bar.close()

    # Print the number of failed uploads
    print(f"\nUpload process completed. Number of failed uploads: {failed_counter.value}")


if __name__ == "__main__":
    # Update the directory and bucket_name to your specific paths
    directory = "/mnt/leonardo/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/UK_Met_Office/UKV_ext/t5"
    bucket_name = "solar-pv-nowcasting-data/NWP/UK_Met_Office/UKV_extended"  # Change this to your GCS bucket name
    main(directory, bucket_name)
