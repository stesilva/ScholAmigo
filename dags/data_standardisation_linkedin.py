import warnings
from dotenv import load_dotenv
import boto3
from datetime import datetime
import os
from send_data_to_aws import send_data_to_aws
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, expr, lower, upper, trim, lit
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.functions import explode, transform, struct
import pycountry
from pyspark.sql import functions as F
from pyspark.sql.window import Window


warnings.filterwarnings("ignore")
load_dotenv()

current_year = datetime.now().year
SPARK_JARS_PATH = 'C:/Program Files/spark_jars'


# Title case conversion
@udf(StringType())
def title_case_udf(value):
    if value and isinstance(value, str):
        return " ".join(w.capitalize() for w in value.split())
    return None

# Email validation
email_pattern = re.compile(r'^[a-zA-Z0-9_.+-]+@(gmail|hotmail|example)\.(com|org|net|edu)$')
@udf(StringType())
def validate_email_udf(email):
    if email and email_pattern.match(email):
        return email.lower()
    return None

# Age validation
@udf(IntegerType())
def validate_age_udf(age):
    if age and 18 <= age <= 100:
        return age
    return None

# Country validation
valid_countries = set([c.name for c in pycountry.countries])
country_pattern = re.compile(r'^[A-Za-z\s]+$')
@udf(StringType())
def validate_country_udf(country):
    if country and country_pattern.match(country):
        title = " ".join(w.capitalize() for w in country.split())
        return title if title in valid_countries else None
    return None

# Degree validation
accepted_degrees = {'High School', 'Bachelor', 'Master', 'PhD'}
@udf(StringType())
def validate_degree_udf(degree):
    if degree in accepted_degrees:
        return " ".join(w.capitalize() for w in degree.split())
    return None

# Language level validation
accepted_levels = {'Beginner', 'Intermediate', 'Advanced'}
@udf(StringType())
def validate_language_level_udf(level):
    if level in accepted_levels:
        return " ".join(w.capitalize() for w in level.split())
    return None

# Duration parsing
number_pattern = re.compile(r'(\d+(\.\d+)?)')
@udf(FloatType())
def parse_duration_udf(duration):
    if duration:
        match = number_pattern.search(duration)
        if match:
            return float(match.group(1))
    return None

# Year validation
@udf(IntegerType())
def validate_year_udf(year):
    try:
        year_int = int(year)
        return year_int if year_int > 0 else current_year
    except:
        return current_year

# Language level ranking
language_level_ranks = {
    "Beginner": 1,
    "Intermediate": 2,
    "Advanced": 3
}
@udf(IntegerType())
def language_rank_udf(level):
    return language_level_ranks.get(level, 0)

# Keep highest language level
def keep_highest_language_level(df):
    window_spec = Window.partitionBy("email", "languages.language")
    return df.withColumn("language_rank", language_rank_udf(F.col("languages.level"))) \
             .withColumn("max_language_rank", F.max("language_rank").over(window_spec)) \
             .filter(F.col("language_rank") == F.col("max_language_rank")) \
             .drop("language_rank", "max_language_rank")

# Keep latest education
def keep_latest_education(df):
    window_spec = Window.partitionBy("email", "educations.institution_name") \
                           .orderBy(F.desc("educations.graduation_year"))
    return df.withColumn("row_num", F.row_number().over(window_spec)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")

# Keep longest experience
def keep_longest_experience(df):
    window_spec = Window.partitionBy("email", "experiences.company_name") \
                           .orderBy(F.desc("experiences.designations.duration"))
    return df.withColumn("row_num", F.row_number().over(window_spec)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")

# Keep latest license
def keep_latest_license(df):
    window_spec = Window.partitionBy("email", "licenses.license_name") \
                           .orderBy(F.desc("licenses.issued_year"))
    return df.withColumn("row_num", F.row_number().over(window_spec)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")

# Deduplicate based on attribute in array
def keep_unique(array_column, attribute_name):
    return F.expr(f"array_distinct(transform({array_column}, x -> x.{attribute_name}))")


def standardize_linkedin_data(linkedin_data):
    linkedin_data_standardized_clean = linkedin_data

    #remove duplicates based on email
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.dropDuplicates(["email"])

    #Basic scalar fields
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("name", when(col("name").isNotNull(), title_case_udf("name")).otherwise(col("name")))
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("email", when(col("email").isNotNull(), validate_email_udf("email")).otherwise(col("email")))
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("age", when(col("age").isNotNull(), validate_age_udf("age")).otherwise(col("age")))
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("country", when(col("country").isNotNull(), validate_country_udf("country")).otherwise(col("country")))
    
    #experiences: array<struct<company_name, designations: array<struct<designation, duration>>>>
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("experiences", when(col("experiences").isNotNull(),
        transform("experiences", lambda x: struct(
            when(x["company_name"].isNotNull(), title_case_udf(x["company_name"])).otherwise(x["company_name"]).alias("company_name"),
            when(col("experiences.designations").isNotNull(),
                transform(col("experiences.designations"), lambda d: struct(
                    when(col("experiences.designations.designation").isNotNull(), title_case_udf(col("experiences.designations.designation"))).otherwise(col("experiences.designations.designation")).alias("role"),
                    when(col("experiences.designations.duration").isNotNull(), parse_duration_udf(col("experiences.designations.duration"))).otherwise(None).cast(FloatType()).alias("duration")
                ))
            ).otherwise(col("experiences.designations")).alias("designations")
        ))
    ).otherwise(col("experiences")))

    #educations: array<struct<college, degree, graduation_year, major>>
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("educations", when(col("educations").isNotNull(),
        transform("educations", lambda x: struct(
            when(x["college"].isNotNull(), title_case_udf(x["college"])).otherwise(x["college"]).alias("institution_name"),
            when(x["degree"].isNotNull(), validate_degree_udf(x["degree"])).otherwise(x["degree"]).alias("degree"),
            when(x["graduation_year"].isNotNull(), validate_year_udf(x["graduation_year"]))
                .otherwise(lit(current_year)).cast(IntegerType()).alias("graduation_year"),
            when(x["major"].isNotNull(), title_case_udf(x["major"])).otherwise(x["major"]).alias("major")
        ))
    ).otherwise(col("educations")))

    #licenses: array<struct<name, institute, issued_date>>
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("licenses", when(col("licenses").isNotNull(),
        transform("licenses", lambda x: struct(
            when(x["name"].isNotNull(), title_case_udf(x["name"])).otherwise(x["name"]).alias("license_name"),
            when(x["institute"].isNotNull(), title_case_udf(x["institute"])).otherwise(x["institute"]).alias("institute_name"),
            when(x["issued_date"].isNotNull(), validate_year_udf(x["issued_date"])).otherwise(lit(current_year)).cast(IntegerType()).alias("issued_year")
        ))
    ).otherwise(col("licenses")))

    #languages: array<struct<name, level>>
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("languages", when(col("languages").isNotNull(),
        transform("languages", lambda x: struct(
            when(x["name"].isNotNull(), title_case_udf(x["name"])).otherwise(x["name"]).alias("language"),
            when(x["level"].isNotNull(), validate_language_level_udf(x["level"])).otherwise(x["level"]).alias("level")
        ))
    ).otherwise(col("languages")))
    
    #skills: array<struct<name>>
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("skills", when(col("skills").isNotNull(),
        transform("skills", lambda x: struct(
            when(x["name"].isNotNull(), title_case_udf(x["name"])).otherwise(x["name"]).alias("name")
        ))
    ).otherwise(col("skills")))

    #courses: array<struct<course_name, associated_with>>
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("courses", when(col("courses").isNotNull(),
        transform("courses", lambda x: struct(
            when(x["course_name"].isNotNull(), title_case_udf(x["course_name"])).otherwise(x["course_name"]).alias("course_name"),
            when(x["associated_with"].isNotNull(), title_case_udf(x["associated_with"])).otherwise(x["associated_with"]).alias("association_name")
        ))
    ).otherwise(col("courses")))

    #honors: array<struct<honor_name>>
    linkedin_data_standardized_clean = linkedin_data_standardized_clean.withColumn("honors", when(col("honors").isNotNull(),
        transform("honors", lambda x: struct(
            when(x["honor_name"].isNotNull(), title_case_udf(x["honor_name"])).otherwise(x["honor_name"]).alias("honor")
        ))
    ).otherwise(col("honors")))

    linkedin_data_standardized_clean = remove_duplicates(linkedin_data_standardized_clean) 
    return linkedin_data_standardized_clean

#remove duplicates for same name values (e.g language 'Chinese' repeated with level 'Beginner' and 'Advanced')
def remove_duplicates(linkedin_data):
    #Group by email and apply transformations
    window_spec = Window.partitionBy("email").orderBy(F.lit(1))
    linkedin_data_grouped = linkedin_data \
        .withColumn("name", F.first("name").over(window_spec)) \
        .withColumn("country", F.first("country").over(window_spec)) \
        .withColumn("age", F.first("age").over(window_spec)) \
        .withColumn("skills", F.first("skills").over(window_spec)) \
        .withColumn("languages", F.first("languages").over(window_spec)) \
        .withColumn("licenses", F.first("licenses").over(window_spec)) \
        .withColumn("honors", F.first("honors").over(window_spec)) \
        .withColumn("courses", F.first("courses").over(window_spec)) \
        .withColumn("educations", F.first("educations").over(window_spec)) \
        .withColumn("experiences", F.first("experiences").over(window_spec)) \
        .dropDuplicates(["email"])
    
    linkedin_data_standardized_clean_non_duplicated = linkedin_data_grouped \
        .withColumn("skills", F.when(F.col("skills").isNotNull(), keep_unique(F.col("skills"), 'name')).otherwise(F.col("skills"))) \
        .withColumn("courses", F.when(F.col("courses").isNotNull(), keep_unique(F.col("courses"), 'course_name')).otherwise(F.col("courses"))) \
        .withColumn("honors", F.when(F.col("honors").isNotNull(), keep_unique(F.col("honors"), 'honor')).otherwise(F.col("honors"))) \
        .withColumn("languages", F.when(F.col("languages").isNotNull(), keep_highest_language_level(F.col("languages"))).otherwise(F.col("languages"))) \
        .withColumn("educations", F.when(F.col("educations").isNotNull(), keep_latest_education(F.col("educations"))).otherwise(F.col("educations"))) \
        .withColumn("experiences", F.when(F.col("experiences").isNotNull(), keep_longest_experience(F.col("experiences"))).otherwise(F.col("experiences"))) \
        .withColumn("licenses", F.when(F.col("licenses").isNotNull(), keep_latest_license(F.col("licenses"))).otherwise(F.col("licenses")))

    return linkedin_data_standardized_clean_non_duplicated

from datetime import datetime
import boto3
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)

#save the cleaned and standardized data to S3 bucket
def save_data(bucket_name, linkedin_data_standardized_clean_non_duplicated, folder_name):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        s3_file_name = f"{folder_name}{timestamp}_trusted_linkedin_user_data.json"
        linkedin_data_standardized_clean_non_duplicated.coalesce(10).write.mode("append").json(f"s3a://{bucket_name}/{s3_file_name}")

        logging.info("Data saved successfully.")
    except Exception as e:
        logging.error(f"Error during data saving: {e}")

#read linkedin user data to perform data standardisation
def retrive_linkedin_user_data(s3, bucket_name, folder_name):
    #list all files in the bucket/folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name) 

    if 'Contents' not in response:
        logging.error(f"Error: No file found at {bucket_name}")
        return None
    
    #retrive the last modified file
    latest_file = max(response['Contents'], key=lambda obj: obj['LastModified'])['Key']
    latest_file_path = f"s3a://{bucket_name}/{latest_file}" #path to the latest modified file

    os.environ["PYSPARK_PYTHON"] = r"C:/Program Files/Python312/python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:/Program Files/Python312/python.exe"

    #create a spark session
    spark = SparkSession.builder \
        .config("spark.jars", f"{SPARK_JARS_PATH}/hadoop-aws-3.3.1.jar,{SPARK_JARS_PATH}/aws-java-sdk-bundle-1.11.901.jar") \
        .config("spark.python.worker.timeout", "600") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "2") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") #only log error, ignore warnings

    #set AWS credentials
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

    linkedin_data = spark.read.option("multiline", "true").json(latest_file_path) #read json with multiline to handle nested structure

    return linkedin_data

def create_linkedin_trusted_zone():
    try:
        session = boto3.Session(profile_name="bdm_group_member")
        s3 = session.client("s3")
        bucket_name = 'linkedin-data-bdm'
        input_folder_name = 'linkedin_users_data/'
        output_folder_name = 'standardized_linkedin_data/'

        linkedin_user_data = retrive_linkedin_user_data(s3, bucket_name, input_folder_name)
        if linkedin_user_data is None:
            raise ValueError("No valid LinkedIn user data retrieved.")

        linkedin_data_standardized_clean_non_duplicated = standardize_linkedin_data(linkedin_user_data)  #standardize the data

        save_data(bucket_name, linkedin_data_standardized_clean_non_duplicated, output_folder_name)

    except Exception as e:
        logging.error(f"Unexpected error in generate_linkedin: {e}")

if __name__ == "__main__":
    create_linkedin_trusted_zone()
