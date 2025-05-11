import subprocess
import boto3
import os
import traceback
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PG_HOST = "postgres_kafka_consumer"
PG_DB = "kafka"
PG_USER = "kafka"
PG_PASSWORD = "kafka_password"
DUMP_PATH = "/tmp/pg_dump_latest.sql"

#S3 config
AWS_PROFILE = "bdm-2025"
S3_BUCKET = "clickstream-history-ingestion"
S3_PREFIX = "full_pg_dumps/"
RETENTION_DAYS = 3

#identifies and deletes backups in S3 that exceed the retention period
def delete_old_backups(s3, bucket, prefix, days=3):
    """Delete backups older than `days` in s3://bucket/prefix."""
    logger.info(f"Cleaning up old dumps > {days} days in s3://{bucket}/{prefix}")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        logger.info("No backups found.")
        return
    
    now = datetime.now(timezone.utc)
    deleted = 0
    # Loop through the objects in the bucket
    for obj in resp["Contents"]:
        key = obj["Key"]
        age_days = (now - obj["LastModified"]).days
        if age_days > days:
            s3.delete_object(Bucket=bucket, Key=key)
            logger.info(f"Deleted old backup {key} (age={age_days} days)")
            deleted += 1

    logger.info(f"Deleted {deleted} old backups total.")

#create a database dump and upload it to S3 with a timestamped key
def pg_dump_to_s3():
    """Dump entire DB over the network, upload to S3, rotate old backups."""
    try:
        pg_dump_cmd = f"PGPASSWORD={PG_PASSWORD} pg_dump -h {PG_HOST} -U {PG_USER} -d {PG_DB} -f {DUMP_PATH}"
        logger.info(f"Running: {pg_dump_cmd}")
        proc = subprocess.run(["bash", "-c", pg_dump_cmd], capture_output=True, text=True)

        if proc.returncode != 0:
            logger.error(f"pg_dump error: {proc.stderr}")
            raise RuntimeError(f"pg_dump failed with code {proc.returncode}")

        if not os.path.exists(DUMP_PATH) or os.path.getsize(DUMP_PATH) == 0:
            raise RuntimeError("Dump file missing or empty.")

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        s3_key = f"{S3_PREFIX}pg_dump_{timestamp}.sql"
        
        try:
            session = boto3.Session(profile_name=AWS_PROFILE)
            s3 = session.client("s3")
        except Exception as e:
            logger.warning(f"Profile {AWS_PROFILE} not found, trying default creds: {e}")
            s3 = boto3.client("s3")

        logger.info(f"Uploading {DUMP_PATH} to s3://{S3_BUCKET}/{s3_key}")
        with open(DUMP_PATH, "rb") as f:
            s3.upload_fileobj(f, S3_BUCKET, s3_key)

        logger.info("Upload successful.")

        #deletes the local dump file after a successful upload
        os.remove(DUMP_PATH)
        logger.info("Local dump file removed.")

        #rotates old backups in S3 by deleting files older than the specified retention period (3 days)
        delete_old_backups(s3, S3_BUCKET, S3_PREFIX, RETENTION_DAYS)
        logger.info("Backup & cleanup complete.")

    except Exception as e:
        logger.error(f"Backup process failed: {e}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    pg_dump_to_s3()
