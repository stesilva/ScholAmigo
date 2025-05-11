from datetime import datetime
import logging
import os
import warnings
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, expr, size,array, lit, struct, first, row_number, explode, collect_list)
from pyspark.sql.window import Window
import pycountry
import pyspark.sql.functions as F
import tempfile

warnings.filterwarnings("ignore")
load_dotenv()

#Spark confguration
SPARK_JARS_PATH = 'C:/Program Files/spark_jars'

#transform any string to lower case format (e.g. 'Hello World' -> 'hello world')
def lower_case(field):
    return f"lower(trim({field}))"

#used for validating the JSON data
accepted_degrees = ['bachelor', 'master', 'phd', 'postdoctoral researchers', 'faculty']
accepted_language_levels = ['beginner', 'intermediate', 'advanced']
valid_countries = [c.name.lower() for c in pycountry.countries]
email_regex = r'^[a-zA-Z0-9_.+-]+@(gmail|hotmail|example)\.(com|org|net|edu)$' #(e.g. julia@example.edu)
country_pattern = r'^[A-Za-z\s]+$' #only letter and space

#define rank of language level in case of duplicates (e.g 'Portuguese': Beginner and 'Portuguese': Advanced -> maintain 'Portuguese': Advanced)
def language_rank(language_level):
    return (
        f"CASE "
        f"WHEN {language_level} = 'beginner' THEN 1 "
        f"WHEN {language_level} = 'intermediate' THEN 2 "
        f"WHEN {language_level} = 'advanced' THEN 3 "
        f"ELSE 0 END"
    )

#explode the array of structs and replace null or empty arrays with a single null struct
def safe_explode(df, array_col, struct_fields):
    null_struct = struct(*[lit(None).alias(f) for f in struct_fields]) #create a null struct with the same schema as the array elements
    return df.withColumn(
        array_col,
        when(
            (col(array_col).isNull()) | (size(col(array_col)) == 0), #if array_col is null or empty, replace with [null_struct]
            array(null_struct)
        ).otherwise(col(array_col))
    )

#deduplicate array of structs by a specific field (e.g. 'name' in 'skills') and keep the first occurrence of each unique value
#(e.g. 'Python' and 'Python' -> keep only one 'Python')
def deduplicate_array_of_structs_by_field(df, array_col, key_field, group_column="email"):
    struct_fields = [f.name for f in df.schema[array_col].dataType.elementType.fields] 
    df = safe_explode(df, array_col, struct_fields)
    exploded = df.withColumn(f"{array_col}_exploded", explode(col(array_col))) #for each element in the array, create a new row
    exploded = exploded.withColumn(f"{key_field}_for_dedupe", col(f"{array_col}_exploded.{key_field}")) #create a new column for the key field to deduplicate by
    deduped = exploded.dropDuplicates([group_column, f"{key_field}_for_dedupe"]) #deduplicate by the key field and group column
    other_cols = [c for c in df.columns if c not in [array_col, group_column]] #other columns to keep in the final result
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols] #keep the first occurrence of each unique value in the other columns
    agg_exprs.append(collect_list(f"{array_col}_exploded").alias(array_col)) #collect the deduplicated array of structs
    result = deduped.groupBy(group_column).agg(*agg_exprs) #group by the group column and aggregate the other columns
    result = result.withColumn(
        array_col,
        when(
            (size(col(array_col)) == 1) & (col(f"{array_col}")[0].isNull()),
            lit(None) #replace single null arrays with null
        ).otherwise(col(array_col))
    ) 
    return result

#defined to rank languages by level in case of duplicates (for each user [based on email]) (e.g 'Portuguese': Beginner and 'Portuguese': Advanced -> maintain 'Portuguese': Advanced)
def keep_highest_language_level(df):
    struct_fields = [f.name for f in df.schema["languages"].dataType.elementType.fields]
    df = safe_explode(df, "languages", struct_fields)
    exploded = df.withColumn("language", explode(col("languages")))
    exploded = exploded.withColumn("language_rank", expr(language_rank("language.level")))
    window = Window.partitionBy("email", "language.language").orderBy(col("language_rank").desc()) #rank by language level for each user
    exploded = exploded.withColumn("rn", row_number().over(window)) #row number for each language
    filtered = exploded.filter(col("rn") == 1) #only keep the highest level for each language
    other_cols = [c for c in df.columns if c not in ["languages", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("language").alias("languages"))
    result = filtered.groupBy("email").agg(*agg_exprs)
    result = result.withColumn(
        "languages",
        when(
            (size(col("languages")) == 1) & (col("languages")[0].isNull()),
            lit(None) #replace single null arrays with null
        ).otherwise(col("languages"))
    )
    return result

#defined to keep the latest education (for repeated institutions) (e.g. 'Harvard': 2020 and 'Harvard': 2022 -> keep 'Harvard': 2022)
def keep_latest_education(df):
    struct_fields = [f.name for f in df.schema["educations"].dataType.elementType.fields]
    df = safe_explode(df, "educations", struct_fields)
    exploded = df.withColumn("education", explode(col("educations")))
    window = Window.partitionBy("email", "education.institution_name").orderBy(col("education.graduation_year").desc_nulls_last()) #order by graduation year for each user and institution
    exploded = exploded.withColumn("rn", row_number().over(window))
    filtered = exploded.filter(col("rn") == 1)
    other_cols = [c for c in df.columns if c not in ["educations", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("education").alias("educations"))
    result = filtered.groupBy("email").agg(*agg_exprs)
    result = result.withColumn(
        "educations",
        when(
            (size(col("educations")) == 1) & (col("educations")[0].isNull()),
            lit(None)#replace single null arrays with null
        ).otherwise(col("educations"))
    )
    return result

#defined to keep the latest license (for repeated licenses) (e.g. 'Cybersecurity': 2021 and ''Cybersecurity': 2024 -> keep ''Cybersecurity': 2024)
def keep_latest_license(df):
    struct_fields = [f.name for f in df.schema["licenses"].dataType.elementType.fields]
    df = safe_explode(df, "licenses", struct_fields)
    exploded = df.withColumn("license", explode(col("licenses")))
    window = Window.partitionBy("email", "license.license_name").orderBy(col("license.issued_year").desc_nulls_last()) #order by issued year for each user and license
    exploded = exploded.withColumn("rn", row_number().over(window))
    filtered = exploded.filter(col("rn") == 1)
    other_cols = [c for c in df.columns if c not in ["licenses", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("license").alias("licenses"))
    result = filtered.groupBy("email").agg(*agg_exprs)
    result = result.withColumn(
        "licenses",
        when(
            (size(col("licenses")) == 1) & (col("licenses")[0].isNull()),
            lit(None) #replace single null arrays with null
        ).otherwise(col("licenses"))
    )
    return result


#defined to keep the longest experience (for repeated companies) (e.g. 'Google': 2020-2022 and 'Google': 2018-2021 -> keep 'Google': 2020-2022)
def keep_longest_experience(df):
    struct_fields = [f.name for f in df.schema["experiences"].dataType.elementType.fields]
    df = safe_explode(df, "experiences", struct_fields)
    exploded = df.withColumn("experience", explode(col("experiences")))
    duration_num = F.regexp_extract(F.col("experience.designations")[0]["duration"], r"([0-9.]+)", 1).cast("float") #extract the number from the duration string (e.g. '2 years' -> 2.0)
    exploded = exploded.withColumn("duration", duration_num)
    window = Window.partitionBy("email", "experience.company_name").orderBy(col("duration").desc_nulls_last()) #order by duration for each user and company
    exploded = exploded.withColumn("rn", row_number().over(window))
    filtered = exploded.filter(col("rn") == 1)
    other_cols = [c for c in df.columns if c not in ["experiences", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("experience").alias("experiences"))
    result = filtered.groupBy("email").agg(*agg_exprs)
    result = result.withColumn(
        "experiences",
        when(
            (size(col("experiences")) == 1) & (col("experiences")[0].isNull()),
            lit(None) #replace single null arrays with null
        ).otherwise(col("experiences"))
    )
    return result


#clean and standardize the LinkedIn data
def standardize_linkedin_data(df):
    
    #standardize attributes and check bussiness rules
    df = df.withColumn(
        "name", expr(lower_case("name")).otherwise(None)
    ).withColumn(
        "email", when(
            col("email").rlike(email_regex),
            expr(lower_case("email"))
        ).otherwise(None)
    ).withColumn(
        "age", when(
            (col("age").cast("int") >= 18) & (col("age").cast("int") <= 100),
            col("age").cast("int")
        ).otherwise(None)
    ) #age constraint (18 <= age <= 100)

    #check if the country is valid (only letters and spaces) and if it is in the list of valid countries
    country_title = lower_case("country")
    df = df.withColumn(
        "country",
        when(
            (col("country").rlike(country_pattern)) & (expr(country_title).isin(*valid_countries)),
            expr(country_title)
        )
    )

    #experiences: lower case and cast durations
    df = df.withColumn(
        "experiences",
        expr(f"""
            transform(experiences, x ->
                named_struct(
                    'company_name', {lower_case('x.company_name')},
                    'designations', transform(x.designations, d ->
                        named_struct(
                            'role', {lower_case('d.designation')},
                            'duration', cast(regexp_extract(d.duration, r'([0-9]+(?:\\.[0-9]+)?)', 1) as float)
                        )
                    )
                )
            )
        """)
    )

    #eucations: lower case and degree validation
    degrees_list = ",".join([f"'{d}'" for d in accepted_degrees])
    df = df.withColumn(
        "educations",
        expr(f"""
            transform(educations, x ->
                named_struct(
                    'institution_name', {lower_case('x.college')},
                    'degree', case when {lower_case('x.degree')} in ({degrees_list}) then {lower_case('x.degree')} else null end,
                    'graduation_year', cast(x.graduation_year as int),
                    'major', {lower_case('x.major')}
                )
            )
        """)
    )

    #licenses: lower case and year
    df = df.withColumn(
        "licenses",
        expr(f"""
            transform(licenses, x ->
                named_struct(
                    'license_name', {lower_case('x.name')},
                    'institute_name', {lower_case('x.institute')},
                    'issued_year', cast(x.issued_date as int)
                )
            )
        """)
    )

    #languages: lower case and level validation
    levels_list = ",".join([f"'{l}'" for l in accepted_language_levels])
    df = df.withColumn(
        "languages",
        expr(f"""
            transform(languages, x ->
                named_struct(
                    'language', {lower_case('x.name')},
                    'level', case when {lower_case('x.level')} in ({levels_list}) then {lower_case('x.level')} else null end
                )
            )
        """)
    )

    #skills: lower case
    df = df.withColumn(
        "skills",
        expr(f"""
            transform(skills, x ->
                named_struct(
                    'name', {lower_case('x.name')}
                )
            )
        """)
    )

    #courses: lower case
    df = df.withColumn(
        "courses",
        expr(f"""
            transform(courses, x ->
                named_struct(
                    'course_name', {lower_case('x.course_name')},
                    'association_name', {lower_case('x.associated_with')}
                )
            )
        """)
    )

    #honors: lower case
    df = df.withColumn(
        "honors",
        expr(f"""
            transform(honors, x ->
                named_struct(
                    'honor', {lower_case('x.honor_name')}
                )
            )
        """)
    )

    #remove duplicates values within fields
    df = deduplicate_array_of_structs_by_field(df, "skills", "name")
    df = deduplicate_array_of_structs_by_field(df, "courses", "course_name")
    df = deduplicate_array_of_structs_by_field(df, "honors", "honor")

    #keep only the highest/latest/longest in arrays
    df = keep_highest_language_level(df)
    df = keep_latest_education(df)
    df = keep_latest_license(df)
    df = keep_longest_experience(df)
    
    return df

def save_data_with_original_name(df, bucket_name, folder_name, original_filename):
    try:
        #write to a temporary file
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_output = os.path.join(tmpdir, "output_data")
            df.coalesce(1).write.mode("overwrite").json(tmp_output)

            #find the generated JSON file
            json_file_path = None
            for fname in os.listdir(tmp_output):
                if fname.endswith(".json"):
                    json_file_path = os.path.join(tmp_output, fname)
                    break
            if not json_file_path:
                raise FileNotFoundError("No JSON file found in Spark output.")

            #upload to S3 with the original name
            s3 = boto3.client("s3")
            s3_key = f"{folder_name}{original_filename}"
            s3.upload_file(json_file_path, bucket_name, s3_key)

            logging.info(f"Data saved successfully to s3://{bucket_name}/{s3_key}")

    except Exception as e:
        logging.error(f"Error during data saving: {e}")

#retrives the latest file from S3 bucket and folder and loads it into a Spark df (latestes linkedin user data)
def retrive_linkedin_user_data(s3, input_bucket_name, folder_name):
    response = s3.list_objects_v2(Bucket=input_bucket_name, Prefix=folder_name)
    if 'Contents' not in response:
        logging.error(f"Error: No file found at {input_bucket_name}")
        return None
    latest_file_info = max(response['Contents'], key=lambda obj: obj['LastModified'])
    latest_file = latest_file_info['Key']
    original_filename = os.path.basename(latest_file)
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
    
    linkedin_data = spark.read.option("multiline", "true").json(latest_file_path) #allows to read a multilene JSON file
    return linkedin_data, original_filename.split('/')[-1] #return the string with the original filw name

def create_linkedin_trusted_zone():
    try:
        #connect to S3 and retrieve latest linkedin user data
        session = boto3.Session(profile_name="bdm_group_member")
        s3 = session.client("s3")
        input_bucket_name = 'linkedin-data-ingestion'
        input_folder_name = 'linkedin_users_data/'
        output_bucket_name = 'linkedin-data-trusted'
        output_folder_name = 'standardized_linkedin_data/'
        
        linkedin_user_data, original_filename = retrive_linkedin_user_data(s3, input_bucket_name, input_folder_name)
        
        if linkedin_user_data is None:
            raise ValueError("No valid LinkedIn user data retrieved.")
        
        #standardize and clean the data, save it to S3 trusted zone
        linkedin_data_standardized_clean_non_duplicated = standardize_linkedin_data(linkedin_user_data)
        save_data_with_original_name(linkedin_data_standardized_clean_non_duplicated,output_bucket_name,output_folder_name,original_filename)        
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    create_linkedin_trusted_zone()
