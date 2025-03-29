import boto3
s3 = boto3.client('s3')
bucket_name = 'linkedin-users-raw-data'
local_file_path = 'data/profile_data.json'
s3_file_name = 'profile_data.json'

s3.upload_file(
    local_file_path,
    bucket_name,
    s3_file_name
)
