import subprocess
import boto3
from datetime import datetime
import os
import traceback
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

AWS_PROFILE = "bdm_group_member"
S3_BUCKET = "clickstream-historyt-bdm"
S3_KEY_PREFIX = "full_pg_dumps/"
PG_CONTAINER = "postgres_kafka_consumer"
PG_DB = "kafka"
PG_USER = "kafka"
DUMP_PATH = "/tmp/pg_dump_latest.sql"
RETENTION_DAYS = 3

def delete_old_backups(s3_client, bucket, prefix, days=RETENTION_DAYS):
    """Delete backups older than the specified number of days."""
    try:
        logger.info(f"Checking for backups older than {days} days in s3://{bucket}/{prefix}")
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if "Contents" not in response:
            logger.info("No existing backups found")
            return

        now = datetime.now()
        deleted_count = 0
        
        for obj in response["Contents"]:
            last_modified = obj["LastModified"]
            age = (now - last_modified.replace(tzinfo=None)).days
            
            if age > days:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
                deleted_count += 1
                logger.info(f"Deleted old backup: {obj['Key']} (age: {age} days)")
        
        logger.info(f"Deleted {deleted_count} old backups")
    
    except Exception as e:
        logger.error(f"Error in delete_old_backups: {str(e)}")
        traceback.print_exc()

def pg_dump_to_s3():
    """Execute pg_dump and upload the result to S3."""
    try:
        logger.info(f"Starting PostgreSQL dump from container {PG_CONTAINER}")
        
        container_check = subprocess.run(
            ["docker", "inspect", "--format='{{.State.Running}}'", PG_CONTAINER],
            capture_output=True, text=True
        )
        
        if container_check.returncode != 0 or 'true' not in container_check.stdout:
            logger.error(f"Container {PG_CONTAINER} is not running or doesn't exist")
            raise Exception(f"Container {PG_CONTAINER} check failed")
        
        dump_cmd = [
            "docker", "exec", PG_CONTAINER,
            "pg_dump", "-U", PG_USER, "-d", PG_DB, "-f", "/tmp/dump.sql"
        ]
        logger.info(f"Running command: {' '.join(dump_cmd)}")
        
        dump_process = subprocess.run(dump_cmd, capture_output=True, text=True)
        if dump_process.returncode != 0:
            logger.error(f"pg_dump failed with error: {dump_process.stderr}")
            raise Exception(f"pg_dump command failed with exit code {dump_process.returncode}")
        
        copy_cmd = ["docker", "cp", f"{PG_CONTAINER}:/tmp/dump.sql", DUMP_PATH]
        logger.info(f"Running command: {' '.join(copy_cmd)}")
        
        copy_process = subprocess.run(copy_cmd, capture_output=True, text=True)
        if copy_process.returncode != 0:
            logger.error(f"docker cp failed with error: {copy_process.stderr}")
            raise Exception(f"docker cp command failed with exit code {copy_process.returncode}")
        
        if not os.path.exists(DUMP_PATH) or os.path.getsize(DUMP_PATH) == 0:
            logger.error(f"Dump file is missing or empty: {DUMP_PATH}")
            raise Exception("Dump file is missing or empty")
        
        logger.info(f"PostgreSQL dump completed successfully. File size: {os.path.getsize(DUMP_PATH)} bytes")
        
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        s3_key = f"{S3_KEY_PREFIX}pg_dump_{timestamp}.sql"
        
        try:
            session = boto3.Session(profile_name=AWS_PROFILE)
            s3_client = session.client("s3")
        except Exception as e:
            logger.error(f"Failed to create boto3 session: {str(e)}")
            raise
            
        logger.info(f"Uploading dump to s3://{S3_BUCKET}/{s3_key}")
        
        with open(DUMP_PATH, "rb") as f:
            s3_client.upload_fileobj(f, S3_BUCKET, s3_key)
        
        logger.info(f"Upload completed successfully")
        
        os.remove(DUMP_PATH)
        logger.info(f"Removed local dump file: {DUMP_PATH}")
        
        delete_old_backups(s3_client, S3_BUCKET, S3_KEY_PREFIX)
        
        logger.info("Backup process completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed pg_dump backup: {str(e)}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    logger.info("Starting pg_dump_to_s3.py script")
    pg_dump_to_s3()