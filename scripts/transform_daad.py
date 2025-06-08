#!/usr/bin/env python3

import logging
import boto3
import os
import sys
import tempfile
import pyspark
import sys
import pyspark.sql.functions as F
import re
from datetime import datetime
from pyspark.sql.column import Column
from pyspark.sql.functions import col, when, udf, regexp_extract, current_timestamp, date_format, lit, regexp_replace, lower
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# Initialize Spark session with local configuration
SPARK_JARS_PATH='/opt/homebrew/Cellar/apache-spark/3.5.5/libexec/jars'
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/scholarship_data/transform.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
INPUT_BUCKET_NAME = 'scholarship-data-ingestion'
FOLDER_NAME = 'german_scholarships/'
OUTPUT_BUCKET_NAME = 'scholarship-data-trusted'
LOCAL_TMP_DIR = '/tmp/scholarship_data/scholarship'

def get_s3_client():
    """Create and return an S3 client using environment variables for credentials."""
    try:
        session = boto3.Session(profile_name='bdm-2025')
        return session.client('s3')
    except Exception as e:
        logger.error(f"Failed to create S3 client: {str(e)}")
        raise

def download_latest_file(s3_client):
    """Download the latest file from S3 input bucket."""
    try:
        # List objects in the input bucket
        response = s3_client.list_objects_v2(
            Bucket=INPUT_BUCKET_NAME,
            Prefix=FOLDER_NAME
        )
        
        if 'Contents' not in response:
            raise Exception(f"No files found in s3://{INPUT_BUCKET_NAME}/{FOLDER_NAME}")
        
        # Get the latest file
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        file_key = latest_file['Key']
        
        # Create local directory if it doesn't exist
        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
        
        # Generate local file path
        local_file_path = os.path.join(LOCAL_TMP_DIR, os.path.basename(file_key))
        
        logger.info(f"Downloading {file_key} to {local_file_path}")
        
        # Download the file
        s3_client.download_file(INPUT_BUCKET_NAME, file_key, local_file_path)
        
        return local_file_path
    except Exception as e:
        logger.error(f"Failed to download file from S3: {str(e)}")
        raise
def upload_to_s3(s3_client, local_file_path):
    try:
        # Get the actual parquet file (Spark creates a directory structure)
        if os.path.isdir(local_file_path):
            # Find the actual parquet file in the directory
            parquet_files = [f for f in os.listdir(local_file_path) if f.endswith('.parquet')]
            if parquet_files:
                actual_file = os.path.join(local_file_path, parquet_files[0])
            else:
                raise FileNotFoundError("No parquet file found in output directory")
        else:
            actual_file = local_file_path
        
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{FOLDER_NAME}{timestamp}_german_scholarships_trusted.parquet"
        
        # Upload to S3
        s3_client.upload_file(actual_file, OUTPUT_BUCKET_NAME, s3_key)
        
        s3_path = f"s3://{OUTPUT_BUCKET_NAME}/{s3_key}"
        logger.info(f"File uploaded successfully to {s3_path}")
        return s3_path
        
    except Exception as e:
        logger.error(f"Failed to upload file to S3: {str(e)}")
        raise

# Helper functions for data transformation
def clean_text(column):
#     """Standardizes text to lowercase, UTF-8, and trimmed"""
#     return F.trim(F.lower(column))

# def clean_text_full(column: Column) -> Column:
    """
    Standardizes text to lowercase and trimmed while preserving newlines structure
    """
    return regexp_replace(
        regexp_replace(
            F.trim(F.lower(column)),  # From original clean_text: lowercase and trim
            "\\s{2,}", " "            # Replace multiple spaces with single space
        ), 
        "\\n\\s*\\n+", "\n\n"         # Standardize multiple newlines to double newlines
    )

# Create a custom validator function
def validate_status(status):
    valid_statuses = ["Bachelor", "Master", "PhD", "Postdoctoral researchers", "Faculty"]
    return status in valid_statuses

def extract_program_description(overview_text):
    if overview_text is None:
        return None
    
    # Use case-insensitive regex patterns
    programme_pattern = r"(?:programme description|program description)\n([\s\S]*?)(?=\n(?:target group|academic requirements|duration|number of|scholarship value|application papers|application deadline)\n)"
    objective_pattern = r"objective\n([\s\S]*?)(?=\n(?:who can apply\?|what can be funded\?|duration of the funding|value|selection)\n)"
    
    # Try Programme Description pattern first (case-insensitive)
    match = re.search(programme_pattern, overview_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # If not found, try Objective pattern (case-insensitive)
    match = re.search(objective_pattern, overview_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    return "not specified"

def extract_funding_info(overview_text):
    if overview_text is None:
        return None
    
    # Case-insensitive patterns - updated to handle both Value and Scholarship Value
    value_pattern = r"\n(?:scholarship\s+value|value)\n([\s\S]*?)(?=\n(?:selection|further information|to allow you|please note|application papers)\n)"
    funded_pattern = r"\nwhat can be funded\?\n([\s\S]*?)(?=\n(?:duration of the funding|who can apply|duration)\n)"
    
    # Try Value/Scholarship Value section first (case-insensitive)
    match = re.search(value_pattern, overview_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # If not found, try What can be funded (case-insensitive)
    match = re.search(funded_pattern, overview_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    return "not specified"

def extract_deadline_info(procedure_text, overview_text):
    # First check Application Procedure section
    if procedure_text is not None:
        # Look for deadline sections in Application Procedure
        deadline_patterns = [
            r"application deadline\s*(?:\n|:)([\s\S]*?)(?=\n(?:application documents|application location|please note)\s*(?:\n|:)|$)",
            r"deadline\s*(?:\n|:)([\s\S]*?)(?=\n(?:application documents|application location|please note)\s*(?:\n|:)|$)"
        ]
        
        for pattern in deadline_patterns:
            match = re.search(pattern, procedure_text, re.IGNORECASE)
            if match and match.group(1).strip():
                return match.group(1).strip()
    
    # If not found in Application Procedure, check Overview section
    if overview_text is not None:
        # Look for deadline sections in Overview
        overview_deadline_patterns = [
            r"application deadline\s*(?:\n|:)([\s\S]*?)(?=\n(?:application papers|application requirements|scholarship value|duration)\s*(?:\n|:)|$)",
            r"deadline\s*(?:\n|:)([\s\S]*?)(?=\n(?:application papers|application requirements|scholarship value|duration)\s*(?:\n|:)|$)"
        ]
        
        for pattern in overview_deadline_patterns:
            match = re.search(pattern, overview_text, re.IGNORECASE)
            if match and match.group(1).strip():
                return match.group(1).strip()
    
    return "not specified"

def extract_academic_requirements(app_requirements, overview_text):
    # First check if Application Requirements is available and not "N/A"
    if app_requirements is not None and app_requirements != "N/A" and app_requirements.strip() != "":
        return app_requirements.strip()
    
    # If not, look for Academic Requirements section in Overview
    if overview_text is not None:
        academic_req_pattern = r"\nacademic requirements\n([\s\S]*?)(?=\n(?:number of|duration|scholarship value|application papers|application deadline)\n)"
        
        match = re.search(academic_req_pattern, overview_text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return "not specified"

def extract_duration_info(overview_text):
    if overview_text is None:
        return None
    
    # Pattern to match Duration section in Overview
    duration_pattern = r"\nduration\n([\s\S]*?)(?=\n(?:scholarship value|number of scholarships|application papers|application deadline)\n)"
    
    # Try also "Duration of the funding" which appears in some entries
    duration_funding_pattern = r"\nduration of the funding\n([\s\S]*?)(?=\n(?:value|scholarship value|selection|further information)\n)"
    
    # Search for standard Duration section
    match = re.search(duration_pattern, overview_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # If not found, try Duration of the funding
    match = re.search(duration_funding_pattern, overview_text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    return "not specified"

def extract_application_documents(app_procedure, overview_text):
    # First check Application Procedure for documents section
    if app_procedure is not None and app_procedure != "N/A" and app_procedure.strip() != "":
        # Look for common headers about application documents
        doc_patterns = [
            r"\napplication documents\s*(?:\n|:)([\s\S]*?)(?=\n(?:application location|application deadline|please note)\s*(?:\n|:)|$)",
            r"\ndocuments\s*(?:\n|:)([\s\S]*?)(?=\n(?:application location|application deadline|please note)\s*(?:\n|:)|$)",
            r"\nrequired documents\s*(?:\n|:)([\s\S]*?)(?=\n(?:application location|application deadline|please note)\s*(?:\n|:)|$)"
        ]
        
        # Try each pattern
        for pattern in doc_patterns:
            match = re.search(pattern, app_procedure, re.IGNORECASE)
            if match and match.group(1).strip():
                return match.group(1).strip()
    
    # If not found in Application Procedure, check for Application Papers in Overview
    if overview_text is not None:
        app_papers_pattern = r"\napplication papers\n([\s\S]*?)(?=\n(?:application deadline|number of|duration|scholarship value)\n)"
        
        match = re.search(app_papers_pattern, overview_text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    return "not specified"

def extract_website_debug(contact_info, overview, app_procedure, app_requirements):
    # Combine all text to search through
    all_text = " ".join(filter(None, [contact_info or "", overview or "", 
                                    app_procedure or "", app_requirements or ""]))
    
    # List of known valid domains
    valid_domains = [
        r'daad\.de',
        r'study-in-germany\.de',
        r'(?:bafög|bafoeg)\.de',
        r'uni-[\w-]+\.de',
        r'tu-[\w-]+\.de',
        r'fh-[\w-]+\.de',
        r'studienstiftung\.de',
        r'funding-guide\.de',
        r'research-in-germany\.org'
    ]
    
    # Common file extensions and URL endings
    url_endings = r"""
        (?:
            (?:\.(?:html?|php|aspx?|jsp|pdf|doc|docx|txt))?  # Common file extensions
            (?:/)?                                            # Optional trailing slash
            (?=[.,;)\s]|$)                                   # Must be followed by punctuation, space or end of string
        )
    """
    
    # Create patterns that only match known domains with proper endings
    patterns = [
        # Full URLs with known domains
        rf'https?://(?:www\.)?(?:{"|".join(valid_domains)})(?:/[^.,;)\s]*)?{url_endings}',
        # URLs starting with www and known domains
        rf'www\.(?:{"|".join(valid_domains)})(?:/[^.,;)\s]*)?{url_endings}'
    ]
    
    # Find all URLs using patterns
    all_urls = set()
    for pattern in patterns:
        urls = re.findall(pattern, all_text, re.IGNORECASE | re.VERBOSE)
        all_urls.update(urls)
    
    def clean_url(url):
        # Remove any trailing characters that shouldn't be part of the URL
        url = re.sub(r'(?<=/)[^/]*(?:Further|extrainfo\.A)(?=[.,;)\s]|$)', '', url)
        url = re.sub(r'[.,;:"\')}\]]+$', '', url)
        return url.rstrip('/')
    
    # Clean and normalize URLs
    cleaned_urls = []
    for url in all_urls:
        clean_url_str = clean_url(url)
        
        # Skip empty URLs
        if not clean_url_str:
            continue
            
        # Add https:// if missing
        if not clean_url_str.startswith(('http://', 'https://')):
            clean_url_str = 'https://' + clean_url_str
        
        # Additional validation
        if re.search(r'(?:extrainfo|en)(?:\.|/|$)', clean_url_str):  # Common valid endings
            # Avoid duplicates
            if clean_url_str not in cleaned_urls:
                cleaned_urls.append(clean_url_str)
    
    if cleaned_urls:
        return "; ".join(cleaned_urls)
    
    # Special case for Bafög if mentioned without a link
    if "bafög" in all_text.lower() or "bafoeg" in all_text.lower():
        return "https://www.bafög.de"
        
    return "not specified"

def main():
    try:
        # Initialize S3 client
        s3_client = get_s3_client()
        
        # Download latest file from S3
        input_file = download_latest_file(s3_client)
        
        # Create session
        spark = SparkSession.builder \
        .appName('daad_etl') \
        .config("spark.python.worker.timeout", "600") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "2") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.jars", f"{SPARK_JARS_PATH}/hadoop-aws-3.3.1.jar,{SPARK_JARS_PATH}/aws-java-sdk-bundle-1.11.901.jar") \
        .getOrCreate()

        # Set log level and AWS configuration
        spark.sparkContext.setLogLevel("ERROR")
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
        
        # Define schema
        schema = StructType([
            StructField("Country of Origin", StringType(), False),
            StructField("Name", StringType(), False),
            StructField("Status", StringType(), False),
            StructField("Fields of Study", StringType(), False),
            StructField("Overview", StringType(), True),
            StructField("Application Requirements", StringType(), True),
            StructField("Application Procedure", StringType(), True),
            StructField("Application Instructions", StringType(), True),
            StructField("Contact Information", StringType(), True),
        ])
        
        # Read input data
        df = spark.read.option("multiline", "true").json(input_file, schema=schema)

        # # Filter out records with placeholder text for important fields
        df = df.filter(~col("Name").isin(["N/A", "Unknown", ""]))
        df = df.filter(~col("Country of Origin").isin(["N/A", "Unknown", ""]))
        df = df.filter(~col("Status").isin(["N/A", "Unknown", ""]))
        df = df.filter(~col("Fields of Study").isin(["N/A", "Unknown", ""]))

        # # Register types
        validate_status_udf = udf(validate_status, BooleanType())
        extract_program_description_udf = udf(extract_program_description, StringType())
        extract_funding_info_udf = udf(extract_funding_info, StringType())
        extract_deadline_udf = udf(extract_deadline_info, StringType())
        extract_academic_requirements_udf = udf(extract_academic_requirements, StringType())
        extract_duration_udf = udf(extract_duration_info, StringType())
        extract_documents_udf = udf(extract_application_documents, StringType())
        extract_website_udf = udf(extract_website_debug, StringType())

        # Apply functions
        df = df.filter(validate_status_udf(col("Status")))

        df = df.withColumn(
            "scholarship_name",
            clean_text(regexp_replace(col("Name"), "\\s*•\\s*DAAD$", ""))
        ).drop("Name")

        df = df.withColumn("description", 
                        clean_text(extract_program_description_udf(col("Overview"))))

        df = df.withColumn("program_country", lit("germany"))
        df = df.withColumn(
            "origin_country",
            clean_text(col("Country of Origin"))
        ).drop("Country of Origin")


        df = df.withColumn("funding_info", clean_text(extract_funding_info_udf(col("Overview"))))
        df = df.withColumn("program_level", clean_text(col("Status"))).drop("Status")

        df = df.withColumn(
            "required_level",
            when(col("program_level") == "master", "bachelor")
            .when(col("program_level") == "postdoctoral researchers", "phd")
            .when(col("program_level") == "phd", "master")
            .when(col("program_level") == "bachelor", "high school diploma")
        )
        df = df.withColumn(
            "deadline", 
            clean_text(extract_deadline_udf(col("Application Procedure"), col("Overview")))
        )
        df = df.withColumn("intake", lit("fall"))

        df = df.withColumn(
            "exams", 
            clean_text(extract_academic_requirements_udf(col("Application Requirements"), col("Overview")))
        )

        df = df.withColumn(
            "fields_of_study",
            clean_text(col("Fields of Study"))
        ).drop("Fields of Study")

        df = df.withColumn(
            "duration", 
            clean_text(extract_duration_udf(col("Overview")))
        )

        df = df.withColumn(
            "documents", 
            clean_text(extract_documents_udf(col("Application Procedure"), col("Overview")))
        )

        df = df.withColumn(
            "website",
            extract_website_udf(
                col("Contact Information"), 
                col("Overview"), 
                col("Application Procedure"),
                col("Application Requirements")
            )
        )

        df = df.withColumn(
            "instructions",
            when(
                (clean_text(col("Application Instructions")).isin(["n/a", "na", "unknown", ""])) | 
                col("Application Instructions").isNull(),
                lit("not specified")
            )
            .otherwise(col("Application Instructions"))
        ).drop("Application Instructions")

        df = df.withColumn(
            "last_updated_utc",
            date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )

        columns_to_drop = ["Overview", "Application Procedure", "Application Requirements", "Contact Information"]
        df = df.drop(*columns_to_drop)

        # Save transformed data with specific Parquet options
        output_file = os.path.join(LOCAL_TMP_DIR, "transformed_data.parquet")
        df.coalesce(1).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_file)

        # Upload to S3
        output_path = upload_to_s3(s3_client, output_file)
        logger.info(f"Transformation completed successfully. Output at: {output_path}")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
if __name__ == "__main__":
    main()