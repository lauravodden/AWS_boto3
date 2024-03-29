import boto3
import json
import os
import pandas as pd

cwd = str(os.getcwd()).replace('\\', '/')

destination = r'\destination\path\'

# Initiate s3 client
s3 = boto3.client('s3')

# List all buckets in s3
bucket_response = s3.list_buckets()
buckets = bucket_response["Buckets"]

# Specify a bucket
bucket = 'bucket_name'

# Set date ranges
years = list(range(2022, 2023))  # Increase range to 2022
months = [str(item).zfill(2) for item in list(range(1, 13))]  # 1,13
days = [str(item).zfill(2) for item in list(range(1, 32))]  # 1,32
hours = [str(item).zfill(2) for item in list(range(0, 24))]  # 0,24


def make_new_dir(destination, dir_name):
    print('Checking for existing directory...')
    try:
        os.mkdir(f'{destination}\{dir_name}')
        print(f"Path does not yet exist. Created new file directory named {dir_name} at location {destination}")
    except OSError as error:
        print(f"{dir_name} already exists at location {destination}. Files will be written to this existing location")


def aws_to_dest():
    for year in years:
        year_dir = str(year)
        make_new_dir(destination, dir_name=year_dir)
        for month in months:
            month_dir = f'{str(year)}/{str(month)}'
            make_new_dir(destination, dir_name=month_dir)

            for day in days:
                for hour in hours:
                    print(f"Pulling data for: year: {year}, month: {month}, day: {day}, hour: {hour}")
                    file_prefix = f'KINESIS{year}/{month}/{day}/{hour}/'

                    # Create a reusable Paginator
                    paginator = s3.get_paginator('list_objects')
                    # Create a PageIterator from the Paginator
                    page_iterator = paginator.paginate(Bucket=bucket, Prefix=file_prefix)

                    try:
                        pages = []
                        for page in page_iterator:
                            request_files = page["Contents"]
                            pages.append(request_files)

                        request_files = []
                        for sublist in pages:
                            for item in sublist:
                                request_files.append(item)

                        # Save files to the 'pulled_files' directory
                        for file in request_files:
                            print(f'file: {file}')
                            s3_filename = file["Key"].split('/')[-1]
                            filename = s3_filename.split('-')[2:6]
                            filename = '_'.join(filename)

                            content_object = s3.get_object(Bucket=bucket, Key=file["Key"])
                            file_content = content_object['Body'].read().decode('utf-8')
                            with open(f'{destination}/{year}/{month}/{filename}.jsonl', 'a') as f:
                                print(f'Writing {s3_filename} from AWS to {filename} at selected location')
                                f.write(file_content)

                    except Exception as e:
                        print(e)
                        print('No data.')
                        pass


if __name__ == '__main__':
    aws_to_dest()
