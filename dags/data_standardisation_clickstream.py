import os
import logging
import warnings
import re
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import boto3
import tempfile
from typing import Set


#configurations and S3 buxkets and folders names
SPARK_JARS_PATH = 'C:/Program Files/spark_jars'
PYSPARK_PYTHON_PATH = r"C:/Program Files/Python312/python.exe"
PYSPARK_DRIVER_PYTHON_PATH = r"C:/Program Files/Python312/python.exe"

SRC_BUCKET = "clickstream-history-ingestion"
SRC_PREFIX = "clickstream_history/"
DST_BUCKET = "clickstream-history-trusted"
DST_PREFIX = "standardized_clickstream_history/"

warnings.filterwarnings("ignore")
load_dotenv()
logging.basicConfig(level=logging.INFO)

os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON_PATH

session = boto3.Session()
s3 = session.client("s3")

#create a spark session with the resources and configs
def get_spark_session():
    return SparkSession.builder \
        .appName("S3CleanAndSaveOriginalName") \
        .config("spark.jars", f"{SPARK_JARS_PATH}/hadoop-aws-3.3.1.jar,{SPARK_JARS_PATH}/aws-java-sdk-bundle-1.11.901.jar") \
        .config("spark.python.worker.timeout", "600") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "2") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .getOrCreate()

#basic configuration for S3 access
def configure_spark_for_s3(spark):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext.setLogLevel("ERROR")

#list all folders in a specif S3 bucket (used to check what folder is already processed)
def get_s3_folders(bucket: str, prefix: str) -> Set[str]:
    folders = set()
    paginator = s3.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'): #for each page of results
            for common_prefix in page.get('CommonPrefixes', []): #for each common prefix (folder)
                folder_path = common_prefix['Prefix'] #get the folder path
                folder_name = folder_path[len(prefix):].rstrip('/') #remove the prefix and trailing slash
                if folder_name:
                    folders.add(folder_name)
    except s3.exceptions.NoSuchBucket:
        logging.error(f"Bucket {bucket} does not exist")
    return folders

#list csv files in a certain bucket/folder
def list_csv_files(bucket: str, prefix: str):
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'): #only retrieves CSV files
                yield key

#get the date foler and filenames present in that folder
def extract_date_and_filename(key: str):
    m = re.search(r'(\d{4}-\d{2}-\d{2})/([^/]+\.csv)$', key)
    if m:
        return m.group(1), m.group(2)
    return None, None

#saves files with original folder and name in the destination bucket
def save_df_as_csv_with_original_name(df, dst_bucket, dst_prefix, date_folder, filename):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_output = os.path.join(tmpdir, "data_out")
        df.coalesce(1).write.option("header", True).mode("overwrite").csv(tmp_output)
        #find the generated CSV file
        for fname in os.listdir(tmp_output):
            if fname.endswith(".csv"):
                csv_file_path = os.path.join(tmp_output, fname)
                break
        else:
            raise FileNotFoundError("No CSV file found in Spark output.")

        s3 = boto3.client("s3")
        s3_key = f"{dst_prefix}{date_folder}/{filename}"
        s3.upload_file(csv_file_path, dst_bucket, s3_key)
        logging.info(f"Saved cleaned CSV to s3://{dst_bucket}/{s3_key}")

#perform the cleaning of file (drops rows with null values) and saves it in the destination bucket
def clean_and_save_file(spark, src_bucket, dst_bucket, key, dst_prefix):
    date_folder, filename = extract_date_and_filename(key)
    if not date_folder or not filename:
        logging.warning(f"Skipping file with unexpected path: {key}")
        return
    input_path = f"s3a://{src_bucket}/{key}"
    logging.info(f"Processing {input_path}")
    df = spark.read.option("header", "true").csv(input_path)
    df_clean = df.na.drop()
    save_df_as_csv_with_original_name(df_clean, dst_bucket, dst_prefix, date_folder, filename)


def main():
    spark = get_spark_session()
    configure_spark_for_s3(spark)

    #compare folders and process only those not yet processed
    source_folders = get_s3_folders(SRC_BUCKET, SRC_PREFIX)
    dest_folders = get_s3_folders(DST_BUCKET, DST_PREFIX)
    unmatched_folders = source_folders - dest_folders
    logging.info(f"Found {len(unmatched_folders)} folders to process.")

    for folder in unmatched_folders: #for each date folder not yet processed (data cleaned)
        try:
            folder_prefix = f"{SRC_PREFIX}{folder}/"
            csv_files = list(list_csv_files(SRC_BUCKET, folder_prefix))
            if not csv_files:
                logging.warning(f"No CSV files found in folder: {folder}")
                continue
            for key in csv_files:
                clean_and_save_file(spark, SRC_BUCKET, DST_BUCKET, key, DST_PREFIX)
            logging.info(f"Processed folder: {folder}")
        except Exception as e:
            logging.error(f"Error processing folder {folder}: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()
