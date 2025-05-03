from datetime import datetime
import logging
import os
import warnings
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, expr, split, trim, first, row_number, explode, collect_list
)
from pyspark.sql.window import Window
import pycountry

warnings.filterwarnings("ignore")
load_dotenv()

current_year = datetime.now().year
SPARK_JARS_PATH = 'C:/Program Files/spark_jars'

# --- Helper: Title case as SQL expression ---
def title_case_expr(field):
    return f"concat_ws(' ', transform(split(trim({field}), ' '), x -> concat(upper(substring(x, 1, 1)), lower(substring(x, 2)))) )"

# --- Constants for validation ---
accepted_degrees = ['High School', 'Bachelor', 'Master', 'Phd']
accepted_levels = ['Beginner', 'Intermediate', 'Advanced']
valid_countries = [c.name for c in pycountry.countries]
email_regex = r'^[a-zA-Z0-9_.+-]+@(gmail|hotmail|example)\.(com|org|net|edu)$'
country_pattern = r'^[A-Za-z\s]+$'

# --- Native language level ranking as CASE expression ---
def language_rank_case_expr(level_col):
    return (
        f"CASE "
        f"WHEN {level_col} = 'Beginner' THEN 1 "
        f"WHEN {level_col} = 'Intermediate' THEN 2 "
        f"WHEN {level_col} = 'Advanced' THEN 3 "
        f"ELSE 0 END"
    )

# --- Deduplicate array of structs by a field ---
def deduplicate_array_of_structs_by_field(df, array_col, key_field, group_col="email"):
    exploded = df.withColumn(f"{array_col}_exploded", explode(col(array_col)))
    exploded = exploded.withColumn(f"{key_field}_for_dedupe", col(f"{array_col}_exploded.{key_field}"))
    deduped = exploded.dropDuplicates([group_col, f"{key_field}_for_dedupe"])
    other_cols = [c for c in df.columns if c not in [array_col, group_col]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list(f"{array_col}_exploded").alias(array_col))
    return deduped.groupBy(group_col).agg(*agg_exprs)

# --- Keep highest language level using only native SQL ---
def keep_highest_language_level(df):
    exploded = df.withColumn("language", explode(col("languages")))
    exploded = exploded.withColumn(
        "language_rank",
        expr(language_rank_case_expr("language.level"))
    )
    window = Window.partitionBy("email", "language.language").orderBy(col("language_rank").desc())
    exploded = exploded.withColumn("rn", row_number().over(window))
    filtered = exploded.filter(col("rn") == 1)
    other_cols = [c for c in df.columns if c not in ["languages", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("language").alias("languages"))
    # Do NOT aggregate 'email' here; it's already the groupBy key
    return filtered.groupBy("email").agg(*agg_exprs)

# --- Keep latest education by graduation_year (native) ---
def keep_latest_education(df):
    exploded = df.withColumn("education", explode(col("educations")))
    window = Window.partitionBy("email", "education.institution_name").orderBy(col("education.graduation_year").desc_nulls_last())
    exploded = exploded.withColumn("rn", row_number().over(window))
    filtered = exploded.filter(col("rn") == 1)
    other_cols = [c for c in df.columns if c not in ["educations", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("education").alias("educations"))
    return filtered.groupBy("email").agg(*agg_exprs)

# --- Keep latest license by issued_year (native) ---
def keep_latest_license(df):
    exploded = df.withColumn("license", explode(col("licenses")))
    window = Window.partitionBy("email", "license.license_name").orderBy(col("license.issued_year").desc_nulls_last())
    exploded = exploded.withColumn("rn", row_number().over(window))
    filtered = exploded.filter(col("rn") == 1)
    other_cols = [c for c in df.columns if c not in ["licenses", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("license").alias("licenses"))
    return filtered.groupBy("email").agg(*agg_exprs)

# --- Keep longest experience by duration (native) ---
def keep_longest_experience(df):
    exploded = df.withColumn("experience", explode(col("experiences")))
    # Assume duration is a numeric field in the first designation
    exploded = exploded.withColumn("duration", col("experience.designations")[0]["duration"])
    window = Window.partitionBy("email", "experience.company_name").orderBy(col("duration").desc_nulls_last())
    exploded = exploded.withColumn("rn", row_number().over(window))
    filtered = exploded.filter(col("rn") == 1)
    other_cols = [c for c in df.columns if c not in ["experiences", "email"]]
    agg_exprs = [first(c, ignorenulls=True).alias(c) for c in other_cols]
    agg_exprs.append(collect_list("experience").alias("experiences"))
    return filtered.groupBy("email").agg(*agg_exprs)

# --- Main standardization function ---
def standardize_linkedin_data(df):
    # Scalar fields
    df = df.withColumn("name", expr(title_case_expr("name"))) \
           .withColumn("email", when(col("email").rlike(email_regex), lower(col("email")))) \
           .withColumn("age", when((col("age").cast("int") >= 18) & (col("age").cast("int") <= 100), col("age").cast("int")))

    country_title_expr = title_case_expr("country")
    df = df.withColumn(
        "country",
        when(
            (col("country").rlike(country_pattern)) & (expr(country_title_expr).isin(*valid_countries)),
            expr(country_title_expr)
        )
    )

    # Experiences: title case and cast durations
    df = df.withColumn(
        "experiences",
        expr(f"""
            transform(experiences, x ->
                named_struct(
                    'company_name', {title_case_expr('x.company_name')},
                    'designations', transform(x.designations, d ->
                        named_struct(
                            'role', {title_case_expr('d.designation')},
                            'duration', cast(d.duration as float)
                        )
                    )
                )
            )
        """)
    )

    # Educations: title case and degree validation
    degrees_list = ",".join([f"'{d}'" for d in accepted_degrees])
    df = df.withColumn(
        "educations",
        expr(f"""
            transform(educations, x ->
                named_struct(
                    'institution_name', {title_case_expr('x.college')},
                    'degree', case when {title_case_expr('x.degree')} in ({degrees_list}) then {title_case_expr('x.degree')} else null end,
                    'graduation_year', cast(x.graduation_year as int),
                    'major', {title_case_expr('x.major')}
                )
            )
        """)
    )

    # Licenses: title case and year
    df = df.withColumn(
        "licenses",
        expr(f"""
            transform(licenses, x ->
                named_struct(
                    'license_name', {title_case_expr('x.name')},
                    'institute_name', {title_case_expr('x.institute')},
                    'issued_year', cast(x.issued_date as int)
                )
            )
        """)
    )

    # Languages: title case and level validation
    levels_list = ",".join([f"'{l}'" for l in accepted_levels])
    df = df.withColumn(
        "languages",
        expr(f"""
            transform(languages, x ->
                named_struct(
                    'language', {title_case_expr('x.name')},
                    'level', case when {title_case_expr('x.level')} in ({levels_list}) then {title_case_expr('x.level')} else null end
                )
            )
        """)
    )

    # Skills: title case
    df = df.withColumn(
        "skills",
        expr(f"""
            transform(skills, x ->
                named_struct(
                    'name', {title_case_expr('x.name')}
                )
            )
        """)
    )

    # Courses: title case
    df = df.withColumn(
        "courses",
        expr(f"""
            transform(courses, x ->
                named_struct(
                    'course_name', {title_case_expr('x.course_name')},
                    'association_name', {title_case_expr('x.associated_with')}
                )
            )
        """)
    )

    # Honors: title case
    df = df.withColumn(
        "honors",
        expr(f"""
            transform(honors, x ->
                named_struct(
                    'honor', {title_case_expr('x.honor_name')}
                )
            )
        """)
    )

    # Deduplicate arrays of structs
    df = deduplicate_array_of_structs_by_field(df, "skills", "name")
    df = deduplicate_array_of_structs_by_field(df, "courses", "course_name")
    df = deduplicate_array_of_structs_by_field(df, "honors", "honor")

    # Keep only the highest/latest/longest in arrays
    df = keep_highest_language_level(df)
    df = keep_latest_education(df)
    df = keep_latest_license(df)
    df = keep_longest_experience(df)

    return df

# --- S3 Save ---
def save_data(bucket_name, df, folder_name):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        s3_file_name = f"{folder_name}{timestamp}_trusted_linkedin_user_data.json"
        df.coalesce(10).write.mode("append").json(f"s3a://{bucket_name}/{s3_file_name}")
        logging.info("Data saved successfully.")
    except Exception as e:
        logging.error(f"Error during data saving: {e}")

# --- S3 Retrieve ---
def retrive_linkedin_user_data(s3, bucket_name, folder_name):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    if 'Contents' not in response:
        logging.error(f"Error: No file found at {bucket_name}")
        return None
    latest_file = max(response['Contents'], key=lambda obj: obj['LastModified'])['Key']
    latest_file_path = f"s3a://{bucket_name}/{latest_file}"

    os.environ["PYSPARK_PYTHON"] = r"C:/Program Files/Python312/python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:/Program Files/Python312/python.exe"

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
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    linkedin_data = spark.read.option("multiline", "true").json(latest_file_path)
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
        linkedin_data_standardized_clean_non_duplicated = standardize_linkedin_data(linkedin_user_data)
        save_data(bucket_name, linkedin_data_standardized_clean_non_duplicated, output_folder_name)
    except Exception as e:
        logging.error(f"Unexpected error in generate_linkedin: {e}")

if __name__ == "__main__":
    create_linkedin_trusted_zone()
