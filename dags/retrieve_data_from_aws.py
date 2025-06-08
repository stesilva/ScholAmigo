import boto3
import logging
from typing import Optional, Tuple

def retrieve_latest_file_from_s3(bucket_name: str, folder_name: str, profile_name: str = "bdm-2025") -> Tuple[str, str]:
    """
    Retrieves the latest file from specified S3 bucket and folder.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        folder_name (str): Folder prefix in the bucket
        profile_name (str): AWS profile name to use
        
    Returns:
        Tuple[str, str]: A tuple containing (file_content, file_key)
        
    Raises:
        FileNotFoundError: If no files found in the specified location
        Exception: For other AWS-related errors
    """
    try:
        # Initialize S3 client
        session = boto3.Session(profile_name=profile_name)
        s3 = session.client("s3")
        
        # List objects in the bucket/folder
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_name
        )
        
        if 'Contents' not in response:
            raise FileNotFoundError(f"No files found in {bucket_name}/{folder_name}")
        
        # Get the latest file
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        file_key = latest_file['Key']
        
        # Download the file
        file_content = s3.get_object(
            Bucket=bucket_name,
            Key=file_key
        )['Body'].read()
        
        logging.info(f"Successfully retrieved file {file_key} from {bucket_name}")
        return file_content, file_key
        
    except FileNotFoundError:
        logging.error(f"No files found in {bucket_name}/{folder_name}")
        raise
    except Exception as e:
        logging.error(f"Error retrieving file from S3: {str(e)}", exc_info=True)
        raise

def check_file_exists(bucket_name: str, file_key: str, profile_name: str = "bdm-2025") -> bool:
    """
    Checks if a specific file exists in the S3 bucket.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        file_key (str): Full path/key of the file in the bucket
        profile_name (str): AWS profile name to use
        
    Returns:
        bool: True if file exists, False otherwise
    """
    try:
        session = boto3.Session(profile_name=profile_name)
        s3 = session.client("s3")
        
        s3.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except:
        return False 