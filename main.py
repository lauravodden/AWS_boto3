import boto3  # pip install boto3


# Help:
# https://towardsdatascience.com/how-to-upload-and-download-files-from-aws-s3-using-python-2022-4c9b787b15f2

# Let's use Amazon S3
s3 = boto3.resource("s3")

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

# Download files from bucket
s3.download_file(
    Bucket="sample-bucket-1801", Key="train.csv", Filename="data/downloaded_from_s3.csv"
)

# Upload files to bucket
s3.upload_file(
    Filename="data/downloaded_from_s3.csv",
    Bucket="sample-bucket-1801",
    Key="new_file.csv",
)