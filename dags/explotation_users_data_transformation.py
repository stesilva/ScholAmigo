from dotenv import load_dotenv
import os
import logging
import explotation_users_data_load_neo4j as load_data
import warnings
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, expr, size,array, lit, struct, first, row_number, explode, collect_list)
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import tempfile


# Configure logging
logging.basicConfig(level=logging.INFO)  # Set log level to INFO

# Create logger object
logger = logging.getLogger()
warnings.filterwarnings("ignore")
load_dotenv()

#Spark confguration
SPARK_JARS_PATH = 'C:/Program Files/spark_jars'    
    
#retrives the latest file from S3 bucket and folder and loads it into a Spark df
def retrive_data(s3, input_bucket_name, folder_name):
    response = s3.list_objects_v2(Bucket=input_bucket_name, Prefix=folder_name)
    if 'Contents' not in response:
        logging.error(f"Error: No file found at {input_bucket_name}")
        return None
    latest_file_info = max(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_file = latest_file_info['Key']
    latest_file_path = f"s3a://{input_bucket_name}/{latest_file}"

    #for spark to work properly
    os.environ["PYSPARK_PYTHON"] = r"C:/Program Files/Python312/python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:/Program Files/Python312/python.exe"

    #spark session creation and configuration (allocate resources)
    spark = SparkSession.builder \
        .config("spark.jars", f"{SPARK_JARS_PATH}/hadoop-aws-3.3.1.jar,{SPARK_JARS_PATH}/aws-java-sdk-bundle-1.11.901.jar") \
        .config("spark.python.worker.timeout", "600") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "2") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .getOrCreate()
        
    #hadoop configuration for S3 access    
    spark.sparkContext.setLogLevel("ERROR")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    data = spark.read.option("multiline", "true").json(latest_file_path) #allows to read a multilene JSON file
    return data 

def create_users_exploitation_zone():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    import_path = os.getenv("IMPORT_PATH")
    
    try:
        #connect to S3 and retrieve latest user related data
        session = boto3.Session(profile_name="bdm-2025")
        s3 = session.client("s3")
        linkedin_input_bucket_name = 'linkedin-data-trusted'
        linkedin_input_folder_name = 'standardized_linkedin_data/'
        
        users_input_bucket_name = 'users-data-trusted'
        user_input_folder_name = 'user_profile/'
        alumni_input_folder_name = 'alumni_profile/'

        linkedin_user_data = retrive_data(s3, linkedin_input_bucket_name, linkedin_input_folder_name)
        linkedin_user_data.show(5)
        
        if linkedin_user_data is None:
            raise ValueError("No valid LinkedIn user data retrieved.")
        
        basic_user_data = retrive_data(s3, users_input_bucket_name, user_input_folder_name)
        basic_user_data.show(5)
        
        if basic_user_data is None:
            raise ValueError("No valid basic user data retrieved.")

        basic_alumni_data = retrive_data(s3, users_input_bucket_name, alumni_input_folder_name)
        basic_alumni_data.show(5)
        if basic_alumni_data is None:
            raise ValueError("No valid basic alumni data retrieved.")        
        
           
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
     create_users_exploitation_zone()
    


