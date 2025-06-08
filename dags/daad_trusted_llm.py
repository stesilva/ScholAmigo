import pandas as pd
import re
import json
from typing import Dict, Optional, Tuple, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType
import os
import google.generativeai as genai
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType, ArrayType
import boto3
import logging
import hashlib
import time
from datetime import datetime
from retrieve_data_from_aws import retrieve_latest_file_from_s3
from send_data_to_aws import send_data_to_aws
import group_scholarships

# Constants and Environment Setup
INPUT_BUCKET_NAME = 'scholarship-data-trusted'
INPUT_FOLDER_NAME = 'german_scholarships/'
OUTPUT_BUCKET_NAME = 'scholarship-data-trusted'
OUTPUT_FOLDER_NAME = 'german_scholarships_llm/'
LOCAL_TMP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tmp')

def generate_scholarship_id(scholarship_data: dict) -> str:
    """
    Generate a deterministic ID for a scholarship based on its unique attributes.
    
    Args:
        scholarship_data: Dictionary containing scholarship information
        
    Returns:
        A deterministic ID string
    """
    # Combine relevant fields that should uniquely identify a scholarship
    unique_attributes = [
        scholarship_data.get('scholarship_name', ''),
        scholarship_data.get('program_country', ''),
        scholarship_data.get('program_level', ''),
        scholarship_data.get('fields_of_study', '')
    ]
    
    # Create a string combining all attributes
    combined_string = '|'.join(str(attr).lower().strip() for attr in unique_attributes)
    
    # Generate a hash of the combined string
    hash_object = hashlib.sha256(combined_string.encode())
    # Take first 16 characters of the hash for a shorter ID
    return hash_object.hexdigest()[:16]

def create_spark_session():
    """Create and configure Spark session with optimized memory settings"""
    SPARK_JARS_PATH = '/opt/homebrew/Cellar/apache-spark/3.5.5/libexec/jars'
    
    spark = SparkSession.builder \
        .appName('funding_extraction') \
        .config("spark.python.worker.timeout", "600") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "2") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.jars", f"{SPARK_JARS_PATH}/hadoop-aws-3.3.1.jar,{SPARK_JARS_PATH}/aws-java-sdk-bundle-1.11.901.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")
    
    return spark


def load_model():
    """
    Initialize Gemini API
    """
    if not '':
        raise ValueError(
            "GOOGLE_API_KEY environment variable not set. "
            "Please set it with your Google API key: "
            "export GOOGLE_API_KEY='your_api_key_here'"
        )

    try:
        genai.configure(api_key='')
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        return model
    except Exception as e:
        raise Exception(f"Error initializing Gemini: {str(e)}")
    
def main():
    """
    Extract structured information from scholarships using Gemini API
    """
    spark = None
    try:
        # Initialize Spark session
        spark = create_spark_session()
        
        # Get latest file from S3 input bucket
        print(f"Retrieving latest file from {INPUT_BUCKET_NAME}/{INPUT_FOLDER_NAME}...")
        _, input_file_key = retrieve_latest_file_from_s3(
            bucket_name=INPUT_BUCKET_NAME,
            folder_name=INPUT_FOLDER_NAME
        )
        
        # Remove _SUCCESS from the path if present
        input_file_key = input_file_key.replace('/_SUCCESS', '')
        
        # Create local temp directory if it doesn't exist
        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
        
        # Create paths for temporary files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_local_file = os.path.join(LOCAL_TMP_DIR, f'input_{timestamp}.parquet')
        grouped_local_file = os.path.join(LOCAL_TMP_DIR, f'grouped_{timestamp}.parquet')
        
        # Read directly from S3
        print(f"Reading input Parquet file from S3: {input_file_key}...")
        s3_path = f"s3a://{INPUT_BUCKET_NAME}/{input_file_key}"
        df = spark.read.option("encoding", "UTF-8").parquet(s3_path)
        
        # Show some statistics
        print("\nInput data statistics:")
        print(f"Total records: {df.count()}")
        print("\nSchema:")
        df.printSchema()
        print("\nSample data:")
        df.show(2, truncate=False)
        
        df.write.mode("overwrite").parquet(input_local_file)
        
        # Group scholarships by country
        print("Grouping scholarships by country...")
        group_scholarships.group_scholarships(input_local_file, grouped_local_file)
        
        # Stop Spark before reading grouped data to avoid conflicts
        if spark:
            spark.stop()
            spark = None
        
        # Initialize Gemini
        print("Initializing Gemini...")
        model = load_model()
        
        # Read the grouped data using pandas
        print("Reading grouped scholarships...")
        df = pd.read_parquet(grouped_local_file)
        
        # Process all records
        print("Processing scholarship information...")
        results = []
        
        # Process each record
        for _, row in df.iterrows():
            try:
                print(f"\nProcessing: {row['scholarship_name']}")
                original_data = row.to_dict()
                
                # Get raw response from Gemini with retry logic
                max_retries = 3
                retry_delay = 30
                
                for attempt in range(max_retries):
                    try:
                        prompt = f"""
You are a scholarship information extractor. Extract and standardize information from ALL the following text sections into a JSON object.

SCHOLARSHIP NAME:
{row['scholarship_name'] if 'scholarship_name' in row else ''}

DESCRIPTION:
{row['description'] if 'description' in row else ''}

PROGRAM AND ORIGIN COUNTRIES:
Program Country: {row['program_country'] if 'program_country' in row else ''}
Origin Country: {row['origin_country'] if 'origin_country' in row else ''}

FUNDING INFORMATION:
{row['funding_info'] if 'funding_info' in row else ''}

PROGRAM DETAILS:
Level: {row['program_level'] if 'program_level' in row else ''}
Required Level: {row['required_level'] if 'required_level' in row else ''}
Duration: {row['duration'] if 'duration' in row else ''}
Fields of Study: {row['fields_of_study'] if 'fields_of_study' in row else ''}

APPLICATION INFORMATION:
Deadline: {row['deadline'] if 'deadline' in row else ''}
Intake: {row['intake'] if 'intake' in row else ''}
Required Exams: {row['exams'] if 'exams' in row else ''}
Required Documents: {row['documents'] if 'documents' in row else ''}

ADDITIONAL INFORMATION:
Website: {row['website'] if 'website' in row else ''}
Instructions: {row['instructions'] if 'instructions' in row else ''}

Return a JSON object with the following structure:
{{
        "scholarship_coverage": {{
            "tuition": "amount/frequency or 'covered (amount not specified)' or 'not specified'",
            "monthly_allowance": "amount/frequency or 'covered (amount not specified)' or 'not specified'",
            "travel_allowance": "amount/frequency or 'covered (amount not specified)' or 'not specified'",
            "other": "list of other covered expenses"
        }},
        "funding_category": "fully funded or partially funded",
        "deadline": "DD/MM/YYYY or 'not specified'",
        "language_certificates": {{
            "IELTS": "score requirement",
            "TOEFL": "score requirement",
            // other certificates if specified
        }},
        "fields_of_study_code": [
            // Code list of broad fields according to ISCED-F 2013 classification
            // Example: ["01", "02"]
        ],
        "fields_of_study": [
            // Text list of broad fields according to ISCED-F 2013 classification
            // Example: ["education", "arts and humanities"]
        ],
        "study_duration": "duration in years/months or 'not specified'",
        "gpa": "GPA requirement or academic excellence requirement or 'not specified'",
        "list_of_universities": [
            // List of universities mentioned in any section or "not specified"
        ],
        "status": // Status is 'open' if the deadline is after today; otherwise, it is 'closed'.
        "other_information": {{
            "important_dates": {{}},
            "instructions": "any important instructions",
            "additional_notes": "other relevant information"
        }}
}}

IMPORTANT RULES:
1. For funding classification:
   - Mark as "fully funded" if BOTH tuition AND monthly allowance are covered
   - Amount of coverage doesn't affect classification
2. For fields of study:
   - Use the ISCED-F 2013 broad field categories (00 to 10)
   - Map each field to its corresponding broad field category
3. For language certificates:
   - Extract from 'exams' field and any other relevant sections
   - Use standardized format for scores
4. For dates:
   - Use DD/MM/YYYY format
   - Use 'not specified' if not clearly stated
5. For GPA and academic requirements:
   - Search through description, required_level, and other relevant sections
   - Include both numeric GPA requirements and qualitative requirements
6. For universities:
   - Search through all sections (especially description and program_country)
   - Include full university names when possible
7. For missing information:
   - Use 'not specified' for all fields
   - For all string type null values use 'not specified'
8. Everything should be utf-8 encoded
COVERAGE RULES:
Here's an example of classification for coverage:
Input: "The scholarship covers an allowance toward cost of study of 460 EUR per year and monthly living expenses of 992 EUR. Travel costs are covered. Additional benefits include health insurance."

"coverage": {{
"tuition": "460/year",
"monthly allowance": "992/month",
"travel allowance": "covered (amount not specified)",
"other": "health insurance"
}}
- For tuition coverage, consider ANY of these as tuition:
  * Direct mentions of "tuition"
  * "study allowance"
  * "study subsidy"
  * "cost of study"
  * "study costs"
  * "allowance toward the cost of study"
  * Any allowance specifically for educational expenses
- For monthly allowance, also consider:
  * "living expenses"
  * "monthly stipend"
  * "grant dependent on income"
  * Any regular payment for living costs

FUNDING CATEGORY RULES:
- MUST mark as "fully funded" if BOTH of these are true:
  * Tuition is covered (has an amount OR "covered (amount not specified)")
  * Monthly allowance is covered (has an amount OR "covered (amount not specified)")
- MUST mark as "partially funded" ONLY if:
  * Either tuition OR monthly allowance is "not specified" or missing
  * OR if only one of them is covered
- The presence of other benefits (travel, health insurance, etc.) does NOT affect this classification
- The amount of coverage does NOT affect this classification
- If both tuition and monthly allowance are covered in ANY amount, it MUST be "fully funded"

Return only the JSON object, nothing else.
"""
                        response = model.generate_content(prompt)
                        
                        # Parse the response
                        json_match = re.search(r'({.*})', response.text, re.DOTALL)
                        if json_match:
                            extracted_data = json.loads(json_match.group(1))
                            
                            # Create result dictionary with all fields
                            result = {
                                'scholarship_id': generate_scholarship_id(original_data),
                                'scholarship_name': original_data.get('scholarship_name', ''),
                                'description': original_data.get('description', ''),
                                'program_country': original_data.get('program_country', ''),
                                'origin_country': list(original_data.get('origin_countries', [])),  # Convert to list
                                'program_level': original_data.get('program_level', ''),
                                'required_level': original_data.get('required_level', ''),
                                'intake': original_data.get('intake', ''),
                                'documents': original_data.get('documents', ''),
                                'website': original_data.get('website', ''),
                                'last_updated_utc': original_data.get('last_updated_utc', ''),
                                'funding_category': extracted_data.get('funding_category', ''),
                                'deadline': extracted_data.get('deadline', ''),
                                'scholarship_coverage': {str(k): str(v) for k, v in extracted_data.get('scholarship_coverage', {}).items()},
                                'language_certificates': {str(k): str(v) for k, v in extracted_data.get('language_certificates', {}).items()},
                                'fields_of_study_code': list(extracted_data.get('fields_of_study_code', [])),  # Convert to list
                                'study_duration': extracted_data.get('study_duration', ''),
                                'gpa': extracted_data.get('gpa', ''),
                                'list_of_universities': list(extracted_data.get('list_of_universities', [])),  # Convert to list
                                'status': extracted_data.get('status', ''),
                                'other_information': {str(k): str(v) for k, v in extracted_data.get('other_information', {}).items()}
                            }
                            results.append(result)
                            break
                            
                    except Exception as e:
                        if "504" in str(e) and attempt < max_retries - 1:
                            print(f"Attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            print(f"Failed after {attempt + 1} attempts: {str(e)}")
                            break
                
                # Add delay between records
                time.sleep(15)
                
            except Exception as e:
                print(f"Error processing {row['scholarship_name']}: {str(e)}")
                continue
        
        # Create a new Spark session for writing results
        spark = create_spark_session()
        
        # Define schema for the output DataFrame
        schema = StructType([
            # ID field
            StructField("scholarship_id", StringType(), False),
            # Original fields
            StructField("scholarship_name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("program_country", StringType(), True),
            StructField("origin_country", ArrayType(StringType()), True),
            StructField("program_level", StringType(), True),
            StructField("required_level", StringType(), True),
            StructField("intake", StringType(), True),
            StructField("documents", StringType(), True),
            StructField("website", StringType(), True),
            StructField("last_updated_utc", StringType(), True),
            StructField("funding_category", StringType(), True),
            StructField("deadline", StringType(), True),
            StructField("scholarship_coverage", MapType(StringType(), StringType()), True),
            StructField("language_certificates", MapType(StringType(), StringType()), True),
            StructField("fields_of_study_code", ArrayType(StringType()), True),
            StructField("study_duration", StringType(), True),
            StructField("gpa", StringType(), True),
            StructField("list_of_universities", ArrayType(StringType()), True),
            StructField("status", StringType(), True),
            StructField("other_information", MapType(StringType(), StringType()), True)
        ])
        
        # Create DataFrame from results
        result_df = spark.createDataFrame(results, schema=schema)
        
        # Write to temporary local Parquet file with UTF-8 encoding
        final_local_file = os.path.join(LOCAL_TMP_DIR, f'scholarships_processed_{timestamp}.parquet')
        output_s3_key = f"{OUTPUT_FOLDER_NAME}scholarships_processed_{timestamp}.parquet"
        
        print(f"\nWriting results to S3: s3a://{OUTPUT_BUCKET_NAME}/{output_s3_key}...")
        result_df.coalesce(1).write \
            .option("encoding", "UTF-8") \
            .mode("overwrite") \
            .format("parquet") \
            .save(f"s3a://{OUTPUT_BUCKET_NAME}/{output_s3_key}")
        
        # Cleanup temporary files
        # for temp_file in [input_local_file, grouped_local_file, final_local_file]:
        #     if os.path.exists(temp_file):
        #         os.remove(temp_file)
        
        print("\nProcessing completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        return False
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()