#!/bin/bash

# Script to generate AWS S3 links which is used by the data cleaning batch scripts.
# Usage: ./s3_urls_generation.sh <city name or all>

city=$1

aws s3 ls s3://airbnbdataset/$city/ | grep listings.csv | sed -E "s/ +/,/g"| cut -d',' -f4 | sed -e "s/^/s3n:\/\/airbnbdataset\/$city\//" > s3_file_urls.txt
