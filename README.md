# Updating NWP + Sat + PV Data on GCS

Training and inference of ML models is done both locally and in the cloud. This repo contains some of the scripts required to process data into the correct format as well as upload it to Google Cloud Storage, where it can then be transferred onto a disk to be used by a VM.

## Numerical Weather Prediction (NWP) Data

Each NWP is slightly different based on the variables, coverage and other factors as such they are multiple processing scripts and there may even be multiple processing scripts for the same NWP. This is due to OCF potentially repulling data to increase the forecast horizon and include new variables of the same NWP. As such at the top of each script, its primary use case will be stated.

Heres an outline of the process for NWPs:

1. Download the data so that there is one file for each forecast init time.
2. Convert the data into an unzipped Zarr file format and combine all the NWP data variables into a dimension labelled variable.
3. Combine the individual forecast init times together into yearly Zarrs.
4. This is where you also sort, assign types and specify chunking.
5. Inspect the data by making plots and run checks/tests similar to that which might be performed by the data loader for the specific NWP (PVNet uses ocf_datapipes). Use the notebooks in this repo to help.
6. Done! The yearly Zarrs should now be ready to upload to google storage.

### Potential Issues and Notes for NWP

- Issues can arrise if you are still downloading data to the location in which you are using to merge the individual init times from. More on this in 01 in the Issues Log. Solution was to manually remove the files which showed missing data.
- Some of these yearly NWP files can end up being very large (~1Tb). Hence take careful consideration and conduct testing with threads, workers and memory limitations with the Dask client. Also note, additional tasks running on the machine can make a difference (especially if they are also using lots of RAM).
- Another way to track the progress of your the processes is to watch the size of the zarr file grow. You can do this using `du -h` in the appropriate location.

## Satellite Data

Use script called `sat_proc.py` which pulls satellite data from the Google Public Storage. Always a good idea to visualise the data downloaded and to check for NaNs, which do exist in the dataset.

## PV Data

Use script called `gsp_pv_proc.py` can be used to get the latest data from PV Live which is used for OCFs national forecast.

## Moving files to GCS and onto a disk

To upload files to Google Cloud Platform (GCP), you can use the `gsutil` function to go to a Google Cloud Storage (GCS) bucket. The can be done via:

```bash
gsutil cp -r my/folder/path/ gs://your-bucket-name/
```

(RECOMMENDED) For faster uploading, you can use one of the `upload_to_gcs.py` scripts which uses multiprocessing to speed things up.

Once the data is in the GCS bucket, it can be moved to a disk by first SSH'ing onto your VM and attaching the relevant disk (if not already attached) with write permissions. Mount the disk you want to add the new data to with read and write privileges, heres an example:
`sudo mount -o discard,defaults,rw /dev/abc /mnt/disks/abc_data`

If updating an existing disk please note that anyone who has the disk mounted may be required to unmount it in order to change the read/write access.

To move the data from the GCS bucket to the disk, this can be done via:

```bash
gsutil cp -r gs://YOUR_BUCKET_NAME/YOUR_FILE_PATH* /mnt/disks/DISK_NAME/folder
```

The `*` is used to copy all files in that directory.

(RECOMMENDED) For much faster data transfer, you can use the `upload_to_gcs_ecmwf.py` or `upload_to_gcs_mo.py` scripts which again uses multiprocessing to speed things up.
