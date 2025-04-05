import json
import boto3

def send_data_to_aws(data, bucket_name, file_name):

    # Convert JSON data to a string
    json_data = json.dumps(data, ensure_ascii=False, indent=4)

    # Initialize the S3 client
    session = boto3.Session(profile_name="bdm_group_member")
    s3 = session.client("s3")

    # Upload the JSON file to S3
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data,
            ContentType="application/json"
        )
        print(f"File '{file_name}' uploaded successfully to S3 bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")