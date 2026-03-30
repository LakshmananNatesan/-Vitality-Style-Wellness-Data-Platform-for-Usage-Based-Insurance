 
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, hour,
    when, sum, avg, count, round, row_number
)
from pyspark.sql.window import Window

 
spark = SparkSession.builder \
    .appName("Fitbit Vitality Analysis") \
    .getOrCreate()

 
daily_calories = "/scripts/daily_agg/dailyCalories_merged.csv"
daily_intensities = "/scripts/daily_agg/dailyIntensities_merged.csv"
daily_steps = "/scripts/daily_agg/dailySteps_merged.csv"

hourly_calories = "/scripts/hourly_agg/hourlyCalories_merged.csv"
hourly_intensities = "/scripts/hourly_agg/hourlyIntensities_merged.csv"
hourly_steps = "/scripts/hourly_agg/hourlySteps_merged.csv"

 
df_daily_calories = spark.read.csv(daily_calories, header=True, inferSchema=True)
df_daily_intensities = spark.read.csv(daily_intensities, header=True, inferSchema=True)
df_daily_steps = spark.read.csv(daily_steps, header=True, inferSchema=True)

df_hourly_calories = spark.read.csv(hourly_calories, header=True, inferSchema=True)
df_hourly_intensities = spark.read.csv(hourly_intensities, header=True, inferSchema=True)
df_hourly_steps = spark.read.csv(hourly_steps, header=True, inferSchema=True)
 
df_daily_calories = df_daily_calories.withColumn(
    "ActivityDay", to_date("ActivityDay", "M/d/yyyy")
)
df_daily_steps = df_daily_steps.withColumn(
    "ActivityDay", to_date("ActivityDay", "M/d/yyyy")
)
df_daily_intensities = df_daily_intensities.withColumn(
    "ActivityDay", to_date("ActivityDay", "M/d/yyyy")
)

df_hourly_steps = df_hourly_steps.withColumn(
    "ActivityHour_ts",
    to_timestamp("ActivityHour", "M/d/yyyy h:mm:ss a")
).withColumn(
    "activity_date", to_date("ActivityHour_ts")
).withColumn(
    "activity_hour", hour("ActivityHour_ts")
)

df_hourly_intensities = df_hourly_intensities.withColumn(
    "ActivityHour_ts",
    to_timestamp("ActivityHour", "M/d/yyyy h:mm:ss a")
).withColumn(
    "activity_date", to_date("ActivityHour_ts")
).withColumn(
    "activity_hour", hour("ActivityHour_ts")
)

 
daily_fact = (
    df_daily_calories
    .join(df_daily_steps, ["Id", "ActivityDay"], "inner")
    .join(df_daily_intensities, ["Id", "ActivityDay"], "inner")
    .select(
        col("Id").alias("user_id"),
        col("ActivityDay").alias("activity_date"),
        col("Calories").alias("daily_calories"),
        col("Steps").alias("daily_steps"),
        col("SedentaryMinutes"),
        col("LightlyActiveMinutes"),
        col("FairlyActiveMinutes"),
        col("VeryActiveMinutes")
    )
)

 
daily_fact = (
    daily_fact
    .withColumn(
        "met_steps_goal",
        when(col("daily_steps") >= 8000, 1).otherwise(0)
    )
    .withColumn(
        "met_activity_goal",
        when(col("VeryActiveMinutes") >= 30, 1).otherwise(0)
    )
    .withColumn(
        "low_sedentary",
        when(col("SedentaryMinutes") <= 600, 1).otherwise(0)
    )
)
 
user_summary = (
    daily_fact
    .groupBy("user_id")
    .agg(
        count("activity_date").alias("total_days"),
        sum("met_steps_goal").alias("steps_goal_days"),
        sum("met_activity_goal").alias("activity_goal_days")
    )
    .withColumn(
        "steps_compliance_pct",
        round(col("steps_goal_days") / col("total_days") * 100, 1)
    )
)

print("USER SUMMARY SAMPLE")
user_summary.show(5, truncate=False)
 

hourly_fact = (
    df_hourly_steps
    .join(
        df_hourly_intensities,
        ["Id", "ActivityHour_ts", "activity_date", "activity_hour"],
        "inner"
    )
)

 
hourly_fact = hourly_fact.withColumn(
    "is_active_hour",
    when(
        (col("StepTotal") > 0) | (col("TotalIntensity") > 0),
        1
    ).otherwise(0)
)

 
daily_hourly_summary = (
    hourly_fact
    .groupBy("Id", "activity_date")
    .agg(
        sum("is_active_hour").alias("active_hours"),
        avg("StepTotal").alias("avg_steps_per_hour")
    )
)

 
window_spec = Window.partitionBy("Id", "activity_date").orderBy(col("StepTotal").desc())

peak_hour_df = (
    hourly_fact
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .select(
        col("Id"),
        col("activity_date"),
        col("activity_hour").alias("peak_activity_hour")
    )
)


daily_hourly_features = (
    daily_hourly_summary
    .join(peak_hour_df, ["Id", "activity_date"], "left")
)

print("FINAL DAILY HOURLY FEATURES")
daily_hourly_features.show(10, truncate=False)
daily_hourly_features.printSchema()


# to check the engagement ratio
daily_fact = daily_fact.withColumn(
    "is_engaged_day",
    when(
        (col("met_steps_goal") == 1) | (col("met_activity_goal") == 1),
        1
    ).otherwise(0)
)

from pyspark.sql.window import Window

window_7d = (
    Window
    .partitionBy("user_id")
    .orderBy("activity_date")
    .rowsBetween(-6, 0)
)

from pyspark.sql.functions import sum, round

daily_fact = daily_fact.withColumn(
    "engaged_days_last_7",
    sum("is_engaged_day").over(window_7d)
)
daily_fact = daily_fact.withColumn(
    "engagement_ratio_7d",
    round(col("engaged_days_last_7") / 7, 2)
)



# detla steps vs yesterday 
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_prev_day = (
    Window
    .partitionBy("user_id")
    .orderBy("activity_date")
)
daily_fact = daily_fact.withColumn(
    "yesterday_steps",
    lag("daily_steps", 1).over(window_prev_day)
)

daily_fact = daily_fact.withcolumn("difference",col("daily_steps")- col("yesterday_steps"))

daily_fact = daily_fact.withColumn(
    "steps_improved_vs_yesterday",
    when(col("delta_steps_vs_yesterday") > 0, 1).otherwise(0)
)
daily_fact = daily_fact.withColumn(
    "daily_active_minutes",
    col("VeryActiveMinutes") + col("FairlyActiveMinutes")
)


from pyspark.sql.window import Window
from pyspark.sql.functions import sum

window_7d = (
    Window
    .partitionBy("user_id")
    .orderBy("activity_date")
    .rowsBetween(-6, 0)
)

daily_fact = daily_fact.withColumn(
    "active_minutes_7d",
    sum("daily_active_minutes").over(window_7d)
)

from pyspark.sql.functions import lag

daily_fact = daily_fact.withColumn(
    "active_minutes_7d_prev",
    lag("active_minutes_7d", 7).over(
        Window.partitionBy("user_id").orderBy("activity_date")
    )
)
from pyspark.sql.functions import when

daily_fact = daily_fact.withColumn(
    "activity_trend_7d",
    when(col("delta_active_minutes_7d") > 0, "IMPROVING")
    .when(col("delta_active_minutes_7d") < 0, "DECLINING")
    .otherwise("STABLE")
)

# activty_day_streak , # sedentary day streak , # activity_variance_7d

daily_fact = daily_fact.withColumn(
    "row_num",
    row_number().over(w)
)
w = Window.partitionBy("user_id").orderBy("activity_date")

row_number().over(
    Window.partitionBy("user_id", "is_active_day").orderBy("activity_date")
)
daily_fact = daily_fact.withColumn(
    "active_group",
    col("row_num") -
    row_number().over(
        Window.partitionBy("user_id", "is_active_day").orderBy("activity_date")
    )
)
from pyspark.sql.functions import count

activity_streak = (
    daily_fact
    .filter(col("is_active_day") == 1)
    .groupBy("user_id", "active_group")
    .agg(count("*").alias("activity_day_streak"))
)



daily_fact = daily_fact.withColumn(
    "sedentary_group",
    daily_fact["row_num"] -
    row_number().over(
        Window.partitionBy("user_id", "low_sedentary").orderBy("activity_date")
    )
)
sedentary_streak = (
    daily_fact
    .filter(col("low_sedentary") == 0)  # BAD sedentary days
    .groupBy("user_id", "sedentary_group")
    .agg(count("*").alias("sedentary_day_streak"))
)
from pyspark.sql.functions import variance

w_7d = (
    Window
    .partitionBy("user_id")
    .orderBy("activity_date")
    .rowsBetween(-6, 0)
)
daily_fact = daily_fact.withColumn(
    "activity_variance_7d",
    variance("VeryActiveMinutes").over(w_7d)
)


# reward elgibility 

from pyspark.sql.functions import when

reward_fact = daily_features.withColumn(
    "reward_eligible",
    when(
        (col("active_days_7d") >= 5) &
        (col("activity_day_streak") >= 3) &
        (col("sedentary_day_streak") < 4),
        1
    ).otherwise(0)
)











































spark.stop()
print("SPARK FINISHED")


# to calcuate the 7 days engagemnt ration

