import boto3
from datetime import datetime, timedelta, timezone
import traceback

AWS_PROFILE = "bdm_group_member"
S3_BUCKET = "clickstream-history-bdm"
FOLDER_PREFIX = "clickstream_history"
DAYS_TO_KEEP = 7

def delete_old_s3_folders():
    try:
        session = boto3.Session(profile_name=AWS_PROFILE)
        s3 = session.client("s3")

        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=FOLDER_PREFIX)
        if "Contents" not in response:
            print("No S3 objects found.")
            return

        now = datetime.now()
        folders_seen = set()
        for obj in response["Contents"]:
            key = obj["Key"]
            parts = key.split('/')
            if len(parts) >= 2:
                folder = parts[1]
                try:
                    folder_date = datetime.strptime(folder, "%Y-%m-%d").replace()
                    if (now - folder_date).days > DAYS_TO_KEEP:
                        folders_seen.add(folder)
                except ValueError:
                    continue

        for folder in folders_seen:
            print(f"Deleting folder: {folder}/")
            to_delete = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{FOLDER_PREFIX}/{folder}/")
            if "Contents" in to_delete:
                for obj in to_delete["Contents"]:
                    s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
                    print(f"Deleted: {obj['Key']}")

    except Exception as e:
        print("Error in delete_old_s3_folders:", e)
        traceback.print_exc()
        raise

if __name__ == "__main__":
    delete_old_s3_folders()
