import os, logging, datetime, boto3
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               ArrayType, MapType)

S3_BUCKET_TRUSTED = "scholarship-data-trusted"
AWS_PROFILE       = "bdm-2025"
READ_FOLDER       = "erasmus_scholarships"
WRITE_FOLDER      = "erasmus_scholarships_standardized"
FILE_STEM         = "erasmus_scholarship_data"

os.environ["AWS_PROFILE"] = AWS_PROFILE
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s",
                    level=logging.INFO, datefmt="%H:%M:%S")

def canonical_level(col):

    c = F.lower(F.trim(col))

    return (
        F.when(c.rlike(r"^master('?s)?$"),       F.lit("master"))
         .when(c.rlike(r"^bachelor('?s)?$"),     F.lit("bachelor"))
         .when(c.rlike(r"^ph\.?d'?s?$"),         F.lit("phd"))
         .when(c.rlike(r"^postdoctoral"),        F.lit("postdoctoral researchers"))
         .when(c.rlike(r"^faculty"),             F.lit("faculty"))
         .otherwise(F.lit(None))
    )

# spark session
spark = (SparkSession.builder.appName("Erasmus-standardise")
         .config("spark.jars.packages",
                 "org.apache.hadoop:hadoop-aws:3.3.4,"
                 "com.amazonaws:aws-java-sdk-bundle:1.12.665")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "com.amazonaws.auth.profile.ProfileCredentialsProvider")
         .config("spark.hadoop.fs.s3a.aws.profile", AWS_PROFILE)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl",
                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .getOrCreate())

# schema
TARGET_SCHEMA = StructType([
    StructField("scholarship_name",      StringType(), True),
    StructField("description",           StringType(), True),
    StructField("program_country",       ArrayType(StringType()), True),
    StructField("origin_country",        ArrayType(StringType()), True),
    StructField("program_level",         StringType(), True),
    StructField("required_level",        StringType(), True),
    StructField("intake",                StringType(), True),
    StructField("documents",             StringType(), True),
    StructField("website",               StringType(), True),
    StructField("last_updated_utc",      StringType(), True),
    StructField("funding_category",      StringType(), True),
    StructField("deadline",              StringType(), True),
    StructField("scholarship_coverage",  MapType(StringType(), StringType()), True),
    StructField("language_certificates", MapType(StringType(), StringType()), True),
    StructField("fields_of_study_code",  ArrayType(StringType()), True),
    StructField("study_duration",        StringType(), True),
    StructField("gpa",                   StringType(), True),
    StructField("list_of_universities",  ArrayType(StringType()), True),
    StructField("status",                StringType(), True),
    StructField("other_information",     MapType(StringType(), StringType()), True),
    StructField("scholarship_id",        StringType(), True)
])

# locate the newest parquet inside erasmus_scholarships/
s3 = boto3.Session(profile_name=AWS_PROFILE).client("s3")
objects = s3.list_objects_v2(Bucket=S3_BUCKET_TRUSTED,
                             Prefix=f"{READ_FOLDER}/").get("Contents", [])
datasets = [
    o for o in objects
    if o["Key"].endswith("_SUCCESS") and FILE_STEM in o["Key"]
]

latest_prefix = max(datasets, key=lambda x: x["LastModified"])["Key"].rsplit("/", 1)[0]
src_path = f"s3a://{S3_BUCKET_TRUSTED}/{latest_prefix}"
logging.info(f"Reading {src_path}")

df = spark.read.schema(TARGET_SCHEMA).parquet(src_path)

# transform dataframe
longest_origin = (df.select("origin_country")
                    .withColumn("len", F.size("origin_country"))
                    .orderBy(F.desc("len")).limit(1)
                    .collect()[0]["origin_country"])
bv = spark.sparkContext.broadcast(longest_origin)

@F.udf("array<string>")
def lower_array(a): return [s.lower().strip() for s in a] if a else []

@F.udf("array<string>")
def fill_origin(a):
    base = a or bv.value
    return sorted({s.lower().strip() for s in base if s})
json_arr  = F.from_json("documents", ArrayType(StringType()))

df_std = (df
          .withColumn("deadline_date",  F.to_date("deadline",  "dd/MM/yyyy"))
          .withColumn("last_update_ts", F.to_timestamp("last_updated_utc",
                                                       "dd/MM/yyyy HH:mm"))
          .withColumn("scholarship_name", F.lower("scholarship_name"))
          .withColumn("program_country",  lower_array("program_country"))
          .withColumn("origin_country",   fill_origin("origin_country"))
          
          .withColumn("program_level",   canonical_level(F.col("program_level")))
          .withColumn("required_level",  canonical_level(F.col("required_level")))
          .withColumn(
              "documents",
              F.when(F.col("documents").rlike(r'^\s*\['),    
                  F.concat_ws(", ", json_arr)              
              ).otherwise(F.col("documents"))        
          )
          
          .withColumn(
              "status",
              F.when(
                  (F.col("deadline_date").isNotNull()) &
                  (F.col("last_update_ts").isNotNull()) &
                  (F.col("deadline_date") < F.to_date("last_update_ts")),
                  F.lit("closed")
              ).otherwise(F.col("status")))
          .drop("deadline_date", "last_update_ts"))

# build destination key (keep only the file-name part)
dst_dir_name = Path(latest_prefix).name.replace(
    FILE_STEM, FILE_STEM + "_std"
)
dst_path = (
    f"s3a://{S3_BUCKET_TRUSTED}/{WRITE_FOLDER}/{dst_dir_name}"
)

df_std.write.mode("overwrite").parquet(dst_path)
logging.info(f"Wrote {dst_path}")

spark.stop()
