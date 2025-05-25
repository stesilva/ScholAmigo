from dotenv import load_dotenv
import os
import logging
import explotation_users_data_load_neo4j as load_data
import warnings
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,  explode)
import shutil


#Configure logging
logging.basicConfig(level=logging.ERROR)  #Set log level to INFO

#Create logger object
logger = logging.getLogger()
warnings.filterwarnings("ignore")
load_dotenv()

#Spark confguration
SPARK_JARS_PATH = 'C:/Program Files/spark_jars'    


def user_basic_profile_extract_transform(user_profile_df, import_path):
    output_dir = f"basic_info_user_neo4j"
    final_csv_name = "user_basic_information.csv"

    #save the JSON as CSV for neo4j format combability
    user_profile_df.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(output_dir)

    #rename the generate file
    for filename in os.listdir(output_dir):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(output_dir, filename)
            neo4j_import_folder = os.path.join(import_path, final_csv_name)
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(output_dir) #remove local folder
            except FileNotFoundError:
                pass
            break
        

def alumni_basic_profile_extract_transform(alumni_profile_df, import_path):
    output_dir = f"basic_info_alumni_neo4j"
    final_csv_name = "alumni_basic_information.csv"

    #save the JSON as CSV for neo4j format combability
    alumni_profile_df.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(output_dir)

    #rename the generate file
    for filename in os.listdir(output_dir):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(output_dir, filename)
            neo4j_import_folder = os.path.join(import_path, final_csv_name)
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(output_dir) #remove local folder
            except FileNotFoundError:
                pass
            break
    
"""
Exports several CSV files from a structured Spark DataFrame, separating each type of information 
(languages, skills, education, certificates, honors, experiences)
into different files, always keeping the email in each row."""
def linkedin_user_data_extract_transform(linkedin_user_data_df, import_path):
    output_dir = f"linkedin_user_data_neo4j"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    #Languages
    df_languages = linkedin_user_data_df.select(
        "email",
        explode("languages").alias("language_struct")
    ).select(
        "email",
        col("language_struct.language").alias("language"),
        col("language_struct.level").alias("level")
    )
    df_languages.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").option("delimiter", ";").csv(os.path.join(output_dir, "languages"))


    #rename the generate file
    for filename in os.listdir(f'{output_dir}/languages'):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(f'{output_dir}/languages', filename)
            neo4j_import_folder = os.path.join(import_path, 'languages.csv')
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(f'{output_dir}/languages') #remove local folder
            except FileNotFoundError:
                pass
            break

    #Skills
    df_skills = linkedin_user_data_df.select(
        "email",
        explode("skills").alias("skill_struct")
    ).select(
        "email",
        col("skill_struct.name").alias("skill")
    )
    df_skills.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(os.path.join(output_dir, "skills"))
    
    #rename the generate file
    for filename in os.listdir(f'{output_dir}/skills'):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(f'{output_dir}/skills', filename)
            neo4j_import_folder = os.path.join(import_path, 'skills.csv')
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(f'{output_dir}/skills') #remove local folder
            except FileNotFoundError:
                pass
            break

    #Education
    df_education = linkedin_user_data_df.select(
        "email",
        explode("educations").alias("education_struct")
    ).select(
        "email",
        col("education_struct.degree").alias("degree"),
        col("education_struct.institution_name").alias("institution"),
        col("education_struct.graduation_year").alias("graduation_year"),
        col("education_struct.major").alias("program_name")
    )
    df_education.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(os.path.join(output_dir, "education"))
    
    #rename the generate file
    for filename in os.listdir(f'{output_dir}/education'):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(f'{output_dir}/education', filename)
            neo4j_import_folder = os.path.join(import_path, 'education.csv')
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(f'{output_dir}/education') #remove local folder
            except FileNotFoundError:
                pass
            break


    #Certificates (Licenses)
    df_certificates = linkedin_user_data_df.select(
        "email",
        explode("licenses").alias("license_struct")
    ).select(
        "email",
        col("license_struct.license_name").alias("certification_name"),
        col("license_struct.institute_name").alias("associated_with"),
        col("license_struct.issued_year").alias("date")
    )
    df_certificates.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(os.path.join(output_dir, "certificates"))

    #rename the generate file
    for filename in os.listdir(f'{output_dir}/certificates'):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(f'{output_dir}/certificates', filename)
            neo4j_import_folder = os.path.join(import_path, 'certificates.csv')
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(f'{output_dir}/certificates') #remove local folder
            except FileNotFoundError:
                pass
            break

    #Honors
    df_honors = linkedin_user_data_df.select(
        "email",
        explode("honors").alias("honor_struct")
    ).select(
        "email",
        col("honor_struct.honor").alias("honor_name")
    )
    df_honors.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(os.path.join(output_dir, "honors"))

    #rename the generate file
    for filename in os.listdir(f'{output_dir}/honors'):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(f'{output_dir}/honors', filename)
            neo4j_import_folder = os.path.join(import_path, 'honors.csv')
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(f'{output_dir}/honors') #remove local folder
            except FileNotFoundError:
                pass
            break
        
    #Experiences
    from pyspark.sql.functions import explode_outer
    df_experiences = linkedin_user_data_df.select(
        "email",
        explode("experiences").alias("exp_struct")
    ).select(
        "email",
        col("exp_struct.company_name").alias("company_name"),
        explode("exp_struct.designations").alias("role_struct")
    ).select(
        "email",
        "company_name",
        col("role_struct.role").alias("role"),
        col("role_struct.duration").alias("duration")
    )
    df_experiences.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(os.path.join(output_dir, "experiences"))
    
    #rename the generate file
    for filename in os.listdir(f'{output_dir}/experiences'):
        if filename.startswith("part-") and filename.endswith(".csv"):
            local_output_folder = os.path.join(f'{output_dir}/experiences', filename)
            neo4j_import_folder = os.path.join(import_path, 'experiences.csv')
            try:
                os.remove(neo4j_import_folder) #remove previous file
            except FileNotFoundError:
                pass
            shutil.move(local_output_folder, neo4j_import_folder) #move file to neo4j import folder
            try:
                shutil.rmtree(f'{output_dir}/experiences') #remove local folder
                shutil.rmtree(output_dir) #remove local folder
            except FileNotFoundError:
                pass
            break

def retrive_data_locally(path):
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
        
    spark.sparkContext.setLogLevel("ERROR")
    data = spark.read.option("multiline", "true").json(path) #allows to read a multilene JSON file
    return data 

    
#retrives the latest file from S3 bucket and folder and loads it into a Spark linkedin_user_data_df
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
    db_name = os.getenv("NEO4J_DB")
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
                
        #basic_user_data = retrive_data(s3, users_input_bucket_name, user_input_folder_name)
        basic_user_data = retrive_data_locally('outputs/users/user_basic_profiles.json')        
        if basic_user_data is None:
            raise ValueError("No valid basic user data retrieved.")

        #basic_alumni_data = retrive_data(s3, users_input_bucket_name, alumni_input_folder_name)
        basic_alumni_data = retrive_data_locally('outputs/users/alumni_basic_profiles.json')
        if basic_alumni_data is None:
            raise ValueError("No valid basic alumni data retrieved.")  

        #linkedin_user_data = retrive_data(s3, linkedin_input_bucket_name, linkedin_input_folder_name)
        linkedin_user_data = retrive_data_locally('outputs/linkedin/2025-05-11_16-14_linkedin_profile_data.json')
        if linkedin_user_data is None:
            raise ValueError("No valid LinkedIn user data retrieved.")
        
        
        #extract the data from the spark df, do any needed transformation (df explosion) and then loads data from CSV file to Neo4j
        user_basic_profile_extract_transform(basic_user_data, import_path)
        alumni_basic_profile_extract_transform(basic_alumni_data, import_path)
        linkedin_user_data_extract_transform(linkedin_user_data,import_path)
        load_data.connect_load_neo4j(uri, user, password, db_name)

      
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
     create_users_exploitation_zone()
    


