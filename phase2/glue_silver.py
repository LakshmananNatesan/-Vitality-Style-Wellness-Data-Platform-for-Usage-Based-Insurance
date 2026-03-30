import sys
import hashlib
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ================================
# INIT GLUE
# ================================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ================================
# CONFIG
# ================================
BRONZE_PATH = "s3://fitbit.bronze.6006/minute_data/2026/03/18/14/"
SILVER_PATH = "s3://fitbit.silver.6006/minute_data/"

print("=" * 55)
print("Silver Glue Job Starting...")
print(f"Reading from : {BRONZE_PATH}")
print(f"Writing to   : {SILVER_PATH}")
print("=" * 55)

# ================================
# STEP 1 - READ BRONZE DATA
# ================================
print("Step 1: Reading Bronze JSON files...")

df = spark.read \
    .option("multiline", "false") \
    .json(BRONZE_PATH)

total_bronze = df.count()
print(f"Total rows read from Bronze: {total_bronze}")
print("Bronze Schema:")
df.printSchema()

# ================================
# STEP 2 - FIX DATA TYPES
# ================================
print("Step 2: Fixing data types...")

df = df \
    .withColumn("steps",
        F.col("steps").cast(IntegerType())) \
    .withColumn("calories",
        F.col("calories").cast(DoubleType())) \
    .withColumn("intensity",
        F.col("intensity").cast(IntegerType())) \
    .withColumn("sleep_value",
        F.col("sleep_value").cast(IntegerType()))

print("Data types fixed!")

# ================================
# STEP 3 - FIX TIMESTAMP FORMAT
# ================================
print("Step 3: Standardizing timestamps...")

df = df.withColumn(
    "timestamp",
    F.to_timestamp(
        F.col("timestamp"),
        "M/d/yyyy h:mm:ss a"
    )
)

null_count = df.filter(F.col("timestamp").isNull()).count()
print(f"Null timestamps found: {null_count}")

# Drop null timestamps
df = df.filter(F.col("timestamp").isNotNull())

# Extract year, month, day
df = df \
    .withColumn("year",  F.year(F.col("timestamp"))) \
    .withColumn("month", F.month(F.col("timestamp"))) \
    .withColumn("day",   F.dayofmonth(F.col("timestamp")))

print("Timestamps standardized!")

# ================================
# STEP 4 - PII MASKING
# ================================
print("Step 4: Masking user IDs (PII)...")

mask_udf = F.udf(
    lambda uid: "USR_" + hashlib.sha256(
        str(uid).encode()
    ).hexdigest()[:8] if uid else None,
    StringType()
)

df = df.withColumn("user_id", mask_udf(F.col("user_id")))
print("User IDs masked!")

# ================================
# STEP 5 - DATA VALIDATION
# ================================
print("Step 5: Validating data...")

# Count invalid rows
invalid_steps    = df.filter(F.col("steps") < 0).count()
invalid_calories = df.filter(F.col("calories") < 0).count()
null_user_ids    = df.filter(F.col("user_id").isNull()).count()

print(f"Invalid steps    : {invalid_steps}")
print(f"Invalid calories : {invalid_calories}")
print(f"Null user IDs    : {null_user_ids}")

# Remove invalid rows
df = df.filter(F.col("steps") >= 0)
df = df.filter(F.col("calories") >= 0)
df = df.filter(F.col("user_id").isNotNull())

print("Invalid rows removed!")

# ================================
# STEP 6 - ADD DERIVED COLUMNS
# ================================
print("Step 6: Adding derived columns...")

df = df \
    .withColumn("is_active",
        F.when(F.col("steps") > 10, 1).otherwise(0)) \
    .withColumn("is_sleeping",
        F.when(
            F.col("sleep_value").isNotNull(), 1
        ).otherwise(0)) \
    .withColumn("hour_of_day",
        F.hour(F.col("timestamp"))) \
    .withColumn("day_of_week",
        F.dayofweek(F.col("timestamp"))) \
    .withColumn("processed_at",
        F.current_timestamp())

print("Derived columns added!")

# ================================
# STEP 7 - REMOVE DUPLICATES
# ================================
print("Step 7: Removing duplicates...")

before = df.count()
df = df.dropDuplicates(["user_id", "timestamp"])
after = df.count()

print(f"Removed {before - after} duplicates")
print(f"Final row count: {after}")

# ================================
# STEP 8 - SHOW SAMPLE
# ================================
print("Sample rows after transformation:")
df.show(5, truncate=False)
print("Final Schema:")
df.printSchema()

# ================================
# STEP 9 - WRITE TO SILVER
# ================================
print("Step 9: Writing to Silver as Parquet...")

# Dynamic overwrite — only replace
# partitions in this batch
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

df.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .save(SILVER_PATH)

print("=" * 55)
print("SILVER JOB COMPLETE!")
print(f"Silver data at  : {SILVER_PATH}")
print(f"Format          : Parquet")
print(f"Partitioned by  : year / month / day")
print(f"Total rows      : {after}")
print("=" * 55)

job.commit()