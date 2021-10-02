#! /bin/bash

# Script to generate s3 links for data cleaning batch processing
# Usage: ./s3_urls_generation.sh [city_name]

city=$1

aws s3 ls s3://airbnbdataset/$city/ | grep listings.csv | sed -E "s/ +/,/g"| cut -d',' -f4 | sed -e "s/^/s3n:\/\/airbnbdataset\/$city\//" > s3_file_urls.txt
