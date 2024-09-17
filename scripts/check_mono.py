"""
Check the monotonicity of specified dimensions in the dataset.

Parameters:
- file_path (str): The path to the dataset file.
- dimensions (list): A list of dimensions to check for monotonicity.

Returns:
- list: A list of dimensions that are not monotonic, or error if there was an error opening or checking the file.
"""

import glob
import xarray as xr
from tqdm import tqdm


def check_monotonicity(file_path, dimensions):
    """Check if specified dimensions in the dataset are monotonic."""
    try:
        ds = xr.open_dataset(file_path, engine="zarr")
        issues = []
        for dim in dimensions:
            if dim in ds.dims:
                if not ds.indexes[dim].is_monotonic_increasing:
                    issues.append(dim)
        return issues
    except Exception as e:
        print(f"Error opening or checking file {file_path}: {e}")
        return ["error"]


# Directory containing the zarr datasets
tmp_dir = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/UK_Met_Office/UKV_ext/tmp_2"

dimensions_to_check = ["init_time", "step"]

# Gather all zarr files
file_list = glob.glob(f"{tmp_dir}/*.zarr")
print(f"Found {len(file_list)} files to check.")

# Dictionary to store files with issues
files_with_issues = {}

for file in tqdm(file_list, desc="Checking files"):
    issues = check_monotonicity(file, dimensions_to_check)
    if issues:
        files_with_issues[file] = issues

# Print the results
if files_with_issues:
    print("Files with non-monotonic dimensions:")
    for file, issues in files_with_issues.items():
        print(f"{file}: Non-monotonic dimensions - {issues}")
else:
    print("All files have monotonic dimensions as specified.")
