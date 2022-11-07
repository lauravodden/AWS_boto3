import boto3
import json
import os
import pandas as pd

destination = r'\\rstore.qut.edu.au\projects\cif\auspubsphere\dmrc-tc-raw-data_pull'
# Specify a bucket
bucket = 'qut-dmrc-tc-raw-data'
# Initiate s3 client
s3 = boto3.client('s3')


# Create a reusable Paginator
paginator = s3.get_paginator('list_objects')

# Create a PageIterator from the Paginator
page_iterator = paginator.paginate(Bucket=bucket, Prefix='KINESIS2019/11')

pages = []
for page in page_iterator:
    request_files = page["Contents"]
    pages.append(request_files)

request_files = []
for sublist in pages:
    for item in sublist:
        request_files.append(item)

















