"""
Check if all required variables are present in the 'variable' coordinate of the dataset.

Parameters:
- file_path (str): The path to the zarr dataset file.

Returns:
- missing_vars (list): A list of variables that are missing from the 'variable' coordinate.
"""

import glob
import xarray as xr
from tqdm import tqdm

# List of required variables that should be present in the 'variable' coordinate for UKV
# required_variables = [
#     'dlwrf', 'dswrf', 'hcc', 'lcc', 'mcc', 'prate', 'r', 'sde',
#     'si10', 't', 'vis', 'wdir10'
# ]

# List of required variables that should be present in the 'variable' coordinate for ECMWF
required_variables = [
    "t2m",
    "dswrf",
    "dlwrf",
    "sr",
    "duvrs",
    "hcc",
    "lcc",
    "mcc",
    "tcc",
    "sd",
    "u10",
    "u100",
    "v10",
    "v100",
]


def check_variables(file_path):
    """Check if all required variables are present in the 'variable' coordinate of the dataset."""
    try:
        ds = xr.open_dataset(file_path, engine="zarr")
        # Ensure 'variable' is a coordinate in the dataset
        if "variable" in ds.coords:
            available_vars = set(ds.coords["variable"].values)
            missing_vars = [
                var for var in required_variables if var not in available_vars
            ]
            return missing_vars
        else:
            return ["variable coordinate not found"]
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return ["error opening file"]  # Indicate an error occurred


# Directory containing the zarr datasets
# tmp_dir = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/UK_Met_Office/UKV_ext/tmp_2"
tmp_dir = "/mnt/storage_b/data/ocf/solar_pv_nowcasting/nowcasting_dataset_pipeline/NWP/ECMWF/uk_ext/tmp"

# Gather all zarr files
file_list = sorted(glob.glob(f"{tmp_dir}/*.zarr"))
print(f"Found {len(file_list)} files to check.")

# List to store names of files missing any required variables
files_with_issues = []

# Check each file
for file in tqdm(file_list, desc="Checking files"):
    missing_vars = check_variables(file)
    if missing_vars:
        print(f"{file} is missing variables: {missing_vars}")
        files_with_issues.append(file)

# Print the list of files with missing variables
if files_with_issues:
    print("Files with missing variables:")
    print(files_with_issues)
else:
    print("All files contain all required variables.")
