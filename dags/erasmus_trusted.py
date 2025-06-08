# env  
import os, json, re, datetime, logging, time, hashlib, boto3
from pathlib import Path
from dotenv import load_dotenv
import google.generativeai as genai
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    col, collect_list, udf, lower, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, MapType
)

S3_BUCKET_INGESTION = "scholarship-data-ingestion"
S3_BUCKET_TRUSTED   = "scholarship-data-trusted"
AWS_PROFILE         = "bdm-2025"
FOLDER              = "erasmus_scholarships"
PREFIX  = FOLDER.rstrip("/") + "/"
os.environ["AWS_PROFILE"] = AWS_PROFILE

BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO, datefmt="%H:%M:%S"
)

# spark session 
spark = (
    SparkSession.builder
        .appName("Scholarship-ETL")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.665")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.aws.profile", AWS_PROFILE)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
)

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
    StructField("other_information",     MapType(StringType(), StringType()), True)
])

TYPE_BY_FIELD = {f.name: f.dataType for f in TARGET_SCHEMA.fields}

# helpers 
def _fix(val, dtype):
    if isinstance(dtype, ArrayType):
        return list(val) if isinstance(val, (list, tuple)) else []
    if isinstance(dtype, MapType):
        return val if isinstance(val, dict) else {}
    if isinstance(dtype, StringType):
        if isinstance(val, (list, dict)):
            return json.dumps(val, ensure_ascii=False)
        return val or "not specified"
    return val

def clean_dict(d: dict) -> dict:
    now = datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M")
    d.setdefault("last_updated_utc", now)
    return {k: _fix(d.get(k), TYPE_BY_FIELD[k]) for k in TYPE_BY_FIELD}

@udf(returnType=StringType())
def mk_id(name, prog_country, prog_level, codes):
    parts = [
        (name or "").lower().strip(),
        ",".join(sorted(prog_country or [])).lower(),
        (prog_level or "").lower().strip(),
        ",".join(sorted(codes or [])).lower()
    ]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

# load  
boto = boto3.Session(profile_name=AWS_PROFILE).client("s3")
latest = max(
    boto.list_objects_v2(Bucket=S3_BUCKET_INGESTION, Prefix=PREFIX)["Contents"],
    key=lambda x: x["LastModified"]
)["Key"]
src = f"s3a://{S3_BUCKET_INGESTION}/{latest}"
logging.info(f"Reading {src}")

raw_df = (spark.read.option("multiLine", "true").json(src).dropDuplicates())

grouped = (
    raw_df.groupBy("project", "main_link")
          .agg(collect_list("text").alias("snippets"))
          .collect()
)

prompt_tpl = Path(BASE_DIR / "prompts" / "erasmus_prompt.txt").read_text()

rows = []
model = genai.GenerativeModel("gemini-1.5-flash-latest")

for rec in grouped:
    payload = "\n".join(rec.snippets)
    prompt  = prompt_tpl.replace("{payload}", payload)
    try:
        time.sleep(20)
        txt = model.generate_content(prompt).text
        data = json.loads(re.search(r"\{.*\}", txt, re.DOTALL).group(0))
        data["scholarship_name"] = data.get("scholarship_name") or rec.project
        data["website"]          = data.get("website")          or rec.main_link
        rows.append(clean_dict(data))
        logging.info(f"Extracted: {rec.project}")
    except Exception as e:
        logging.error(f"{rec.project}: {e}")

structured_df = spark.createDataFrame(rows, schema=TARGET_SCHEMA) \
    .withColumn("scholarship_id",
                mk_id("scholarship_name", "program_country",
                      "program_level", "fields_of_study_code")) \
    .select(["scholarship_id"] + TARGET_SCHEMA.fieldNames())

# save 
today_tag = datetime.datetime.utcnow().strftime("%Y-%m")
fname      = f"{today_tag}_erasmus_scholarship_data.parquet"
dst_path   = f"s3a://{S3_BUCKET_TRUSTED}/{FOLDER}{fname}"

structured_df.write.mode("overwrite").parquet(dst_path)
logging.info(f"Wrote {dst_path}")
spark.stop()
