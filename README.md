# Updating NWP + Sat + PV Data on GCS
<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square)](#contributors-)
<!-- ALL-CONTRIBUTORS-BADGE:END -->

![ease of contribution: medium](https://img.shields.io/badge/ease%20of%20contribution:%20medium-f4900c)
[![issues badge](https://img.shields.io/github/issues/openclimatefix/forecast-data-prep?color=FFAC5F)](https://github.com/openclimatefix/forecast-data-prep/issues?q=is%3Aissue+is%3Aopen+sort%3Aupdated-desc)

Scripts to process and upload ML training data to Google Cloud Storage for use on VMs.

## Numerical Weather Prediction (NWP) Data

Multiple processing scripts exist for NWPs due to variations in variables, coverage, and forecast horizons. Each script specifies its primary use case at the top.

NWP Processing Steps:

1. Download individual forecast init time files
2. Convert to unzipped Zarr format with combined variables
3. Merge into yearly Zarrs with proper sorting, typing and chunking
4. Validate data through visualization and testing
5. Upload yearly Zarrs to Google Storage

#### Issues and Important Considerations for NWP Processing

- Issues can arise if you are still downloading data to the location where you are merging individual init times from. The solution is to manually remove files showing missing data.
- Some yearly NWP files can be very large (~1TB). Take careful consideration and conduct testing with threads, workers and memory limitations in the Dask client. Note that additional tasks running on the machine can impact performance, especially if they are also using lots of RAM.
- Another way to track process progress is to watch the zarr file size grow using `du -h` in the appropriate location.

## Satellite Data

Satellite data processing is handled by `sat_proc.py`, which downloads satellite imagery data from Google Public Storage and processes it for ML training.

## PV Data

The `gsp_pv_proc.py` script downloads the National PV generation data from Sheffield Solar PV Live, which is used as the target for OCF's national solar power forecast.

## Moving files to GCS and onto a disk

To upload files locally to Google Cloud Platform (GCP), you can use the `gsutil` library. The can be done via:

```bash
gsutil -m cp -r my/folder/path/ gs://your-bucket-name/
```

For potentially faster uploads, you can try the `upload_to_gcs.py` script which uses multiprocessing to speed things up. However sometimes the limitation is the internet or write speed of the disk so it may not be faster.

Once your data is in the GCS bucket, you can transfer it to a disk on your VM. First, SSH into your VM and make sure the target disk is attached with write permissions. Then mount the disk with read and write privileges using the following command:
`sudo mount -o discard,defaults,rw /dev/ABC /mnt/disks/DISK_NAME`

(You will need to know the disk name, which you can find with `lsblk`, and replace `ABC` with the actual disk name).

If updating an existing disk please note that anyone who has the disk mounted will be required to unmount it in order to change the disks read/write access.

To copy data from your GCS bucket to the mounted disk, use the following command:

```bash
gsutil -m cp -r gs://YOUR_BUCKET_NAME/YOUR_FILE_PATH* /mnt/disks/DISK_NAME/folder
```

The `*` is used to copy all files in that directory.

### Issues during upload

If issues arise when uploading, use `rsync` instead to copy the files across if some have already been downloaded. For example:
```bash
gsutil -m rsync -r gs://solar-pv-nowcasting-data/NWP/UK_Met_Office/UKV_extended/UKV_2023.zarr/ /mnt/disks/gcp_data/nwp/ukv/ukv_ext/UKV_2023.zarr/
```

`rsync` synchronizes files by copying only the differences between source and destination. It can be slow because it needs to scan and compare all files first, then transfer the data. For large datasets like NWP files (~1TB), both the scanning and transfer phases take considerable time due to the volume of data involved.

## Contributing and community

- PR's are welcome! See the [Organisation Profile](https://github.com/openclimatefix) for details on contributing
- Find out about our other projects in the [OCF Meta Repo](https://github.com/openclimatefix/ocf-meta-repo)
- Check out the [OCF blog](https://openclimatefix.org/blog) for updates
- Follow OCF on [LinkedIn](https://uk.linkedin.com/company/open-climate-fix)

---

*Part of the [Open Climate Fix](https://github.com/orgs/openclimatefix/people) community.*

[![OCF Logo](https://cdn.prod.website-files.com/62d92550f6774db58d441cca/6324a2038936ecda71599a8b_OCF_Logo_black_trans.png)](https://openclimatefix.org)

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/captainhaddock18"><img src="https://avatars.githubusercontent.com/u/120558797?v=4?s=100" width="100px;" alt="THARAK HEGDE "/><br /><sub><b>THARAK HEGDE </b></sub></a><br /><a href="https://github.com/openclimatefix/forecast-data-prep/commits?author=captainhaddock18" title="Documentation">📖</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!