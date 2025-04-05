import psycopg2
import csv
import io
import boto3
import traceback
from datetime import datetime

PG_CONFIG = {
    'dbname': 'kafka',
    'user': 'kafka',
    'password': 'kafka_password',
    'host': 'postgres_kafka_consumer',
    'port': '5432'
}

S3_BUCKET = "clickstream-history-bdm"
AWS_PROFILE = "bdm_group_member"
S3_FOLDER = "clickstream_history/"


TABLES = ["search_table", "filter_table", "details_table", "faq_table"]

def export_clickstream():
    """
    Export the last hour's rows from Postgres 'kafka' DB to S3 as CSVs.
    Also creates a subfolder for each day in S3.
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()

        session = boto3.Session(profile_name=AWS_PROFILE)
        s3 = session.client("s3")

        day_str = datetime.now().strftime("%Y-%m-%d")
        time_str = datetime.now().strftime("%H-%M")

        for table in TABLES:
            query = f"""
                SELECT *
                FROM {table}
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            #writes the data to CSV files and uploads them to an S3 bucket
            colnames = [desc[0] for desc in cursor.description]
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            writer.writerow(colnames)
            writer.writerows(rows)

            s3_key = f"{S3_FOLDER}{day_str}/{table}_{time_str}.csv"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv"
            )

            print(f"Uploaded {len(rows)} rows from {table} to s3://{S3_BUCKET}/{s3_key}")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error in export_clickstream:", e)
        traceback.print_exc()
        raise

if __name__ == "__main__":
    export_clickstream()

