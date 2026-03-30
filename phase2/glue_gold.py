import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ✅ Updated bucket names
SILVER_PATH = "s3://fitbit.silver.6006/minute_data/"
GOLD_PATH   = "s3://fitbit.gold.6006/daily_summary/"

print("Gold Job Starting...")
print(f"Reading from: {SILVER_PATH}")
print(f"Writing to  : {GOLD_PATH}")

# Read Silver
df = spark.read.parquet(SILVER_PATH)
print(f"Total Silver rows: {df.count()}")

# Aggregate to daily summary
df_gold = df.groupBy(
    "user_id",
    "year",
    "month",
    "day"
).agg(
    F.sum("steps").alias("total_steps"),
    F.sum("calories").alias("total_calories"),
    F.sum("is_active").alias("active_minutes"),
    F.sum("is_sleeping").alias("sleep_minutes"),
    F.avg("intensity").alias("avg_intensity"),
    F.max("steps").alias("max_steps_in_minute"),
    F.count("*").alias("total_minutes_recorded")
)

# Add anomaly score
df_gold = df_gold.withColumn(
    "anomaly_score",
    F.when(F.col("total_steps") > 15000, 0.8)
     .when(F.col("total_steps") > 12000, 0.5)
     .when(F.col("total_steps") == 0,    0.3)
     .otherwise(0.1)
)

# Add activity level
df_gold = df_gold.withColumn(
    "activity_level",
    F.when(F.col("total_steps") >= 10000, "HIGH")
     .when(F.col("total_steps") >= 5000,  "MEDIUM")
     .when(F.col("total_steps") >= 1000,  "LOW")
     .otherwise("SEDENTARY")
)

# Add processed timestamp
df_gold = df_gold.withColumn(
    "processed_at",
    F.current_timestamp()
)

# Round decimal columns
df_gold = df_gold.withColumn(
    "total_calories",
    F.round(F.col("total_calories"), 2)
)
df_gold = df_gold.withColumn(
    "avg_intensity",
    F.round(F.col("avg_intensity"), 4)
)

print("Sample Gold rows:")
df_gold.show(10, truncate=False)
print(f"Total Gold rows: {df_gold.count()}")

# Dynamic overwrite
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

# Write to Gold
df_gold.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .save(GOLD_PATH)

print("GOLD JOB COMPLETE!")
print(f"Gold data at:   {GOLD_PATH}")
print(f"Partitioned by: year/month")

job.commit()