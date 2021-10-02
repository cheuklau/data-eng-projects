#!/bin/bash

# Usage: ./data_fetch.sh [city_name]
# If city_name not provided then fetching data for all cities.
# This script is run by Airflow for this project.

city=$1

# Grabs all of the download links for the specified city.
# Outputs stored in download_links/data_download_links*.txt with entries of the form:
# http://data.insideairbnb.com/australia/sa/barossa-valley/2021-06-27/data/reviews.csv.gz
LINK='http://insideairbnb.com/get-the-data.html'
mkdir -p download_links
if [ -z "$city" ]
then
    echo "WARN: No city specified so downloading data for all cities."
    city="all"
    curl -s $LINK | grep -Eo "http://[^\"]+gz" > download_links/data_download_links_$city.txt
else
    curl -s $LINK | grep -Eo "http://[^\"]+gz" | grep $city > download_links/data_download_links_$city.txt
fi

# Runs src/data_fetch/download_rename.py to download and rename each of the
# download links found in the previous step.
# The data is then decompressed and uploaded to AWS S3.
mkdir -p dataset/$city/
python3 download_rename.py download_links/data_download_links_$city.txt dataset/$city/
# Why decompress gzip before uploading to AWS S3?
# Spark can read gzip directly.
# Source: https://newbedev.com/how-to-read-gz-compressed-file-by-pyspark
# Increases processing time on machine that runs this script (Airflow).
# Increases latency syncing to S3.
# Increases storage requirements in S3.
# Increases latency reading into Spark.
# However, it would require Spark to decompress.
gzip -df dataset/$city/*.gz
aws s3 sync dataset/$city/ s3://airbnbdataset/$city/
