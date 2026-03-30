import boto3
import pandas as pd
import json
import time
from datetime import datetime
from io import StringIO

# ================================
# CONFIG
# ================================
BUCKET_NAME = "fitbit.insurance.analysis"
REGION = "us-east-2"
FIREHOSE_STREAM = "fitbit.firehose.streams"

# S3 file paths
STEPS_FILE = "minute/minute_level_data/minuteStepsNarrow_merged.csv"
CALORIES_FILE = "minute/minute_level_data/minuteCaloriesNarrow_merged.csv"
INTENSITIES_FILE = "minute/minute_level_data/minuteIntensitiesNarrow_merged.csv"
SLEEP_FILE = "minute/minute_level_data/minuteSleep_merged.csv"

# ================================
# LOAD CSV FROM S3
# ================================
def load_csv_from_s3(bucket, key):
    print(f"📂 Loading {key}...")
    s3 = boto3.client('s3', region_name=REGION)
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(content))
    print(f"✅ Loaded {len(df)} rows from {key}")
    return df

# ================================
# VALIDATE EACH ROW
# ================================
def validate_event(event):
    errors = []

    # Check 1 - Required columns exist
    required = ['user_id', 'timestamp', 'steps', 'calories']
    for col in required:
        if col not in event:
            errors.append(f"Missing column: {col}")

    # Check 2 - Null values
    for col in required:
        if event.get(col) is None or str(event.get(col)).strip() == '':
            errors.append(f"Null value in: {col}")

    # Check 3 - Data types
    try:
        int(event.get('steps', 'x'))
    except:
        errors.append("Steps must be integer")

    try:
        float(event.get('calories', 'x'))
    except:
        errors.append("Calories must be float")

    # Check 4 - Timestamp format
    try:
        datetime.strptime(
            str(event.get('timestamp', '')),
            '%m/%d/%Y %I:%M:%S %p'
        )
    except:
        errors.append("Invalid timestamp format")

    # Check 5 - Business rules
    try:
        if int(event.get('steps', 0)) < 0 or int(event.get('steps', 0)) > 200:
            errors.append(f"Steps out of range: {event.get('steps')}")
        if float(event.get('calories', 0)) < 0 or float(event.get('calories', 0)) > 20:
            errors.append(f"Calories out of range: {event.get('calories')}")
    except:
        pass

    return errors

# ================================
# SEND BATCH TO KINESIS FIREHOSE
# ================================
def send_to_firehose(events_batch, batch_number):
    firehose = boto3.client('firehose', region_name=REGION)

    # Firehose expects list of records
    # Each record = JSON string + newline
    records = [
        {'Data': (json.dumps(event) + '\n').encode('utf-8')}
        for event in events_batch
    ]

    # Send batch to Firehose
    # Max 500 records per put_record_batch call
    response = firehose.put_record_batch(
        DeliveryStreamName=FIREHOSE_STREAM,
        Records=records
    )

    # Check how many failed
    failed = response.get('FailedPutCount', 0)

    if failed > 0:
        print(f"⚠️  Batch {batch_number} → {len(records) - failed} sent | {failed} failed")
    else:
        print(f"✅ Batch {batch_number} → {len(records)} records sent to Firehose")

    return failed

# ================================
# MAIN SIMULATOR
# ================================
def simulate_stream():
    print("=" * 55)
    print("🚀 Fitbit Firehose Simulator Starting...")
    print(f"📡 Firehose Stream : {FIREHOSE_STREAM}")
    print(f"🪣  Bronze Bucket   : fitbit-bronze-layer")
    print("=" * 55)

    # ---- Load all files ----
    steps_df      = load_csv_from_s3(BUCKET_NAME, STEPS_FILE)
    calories_df   = load_csv_from_s3(BUCKET_NAME, CALORIES_FILE)
    intensities_df = load_csv_from_s3(BUCKET_NAME, INTENSITIES_FILE)
    sleep_df      = load_csv_from_s3(BUCKET_NAME, SLEEP_FILE)

    # ---- Merge all on user_id + timestamp ----
    print("\n🔗 Merging all files...")

    # Merge steps + calories
    merged_df = pd.merge(
        steps_df,
        calories_df,
        on=['Id', 'ActivityMinute'],
        how='inner'
    )

    # Merge + intensities
    merged_df = pd.merge(
        merged_df,
        intensities_df,
        on=['Id', 'ActivityMinute'],
        how='inner'
    )
    sleep_df = sleep_df.rename(columns={
        'date': 'ActivityMinute',
        'value': 'sleep_value',
        'logId': 'log_id'
    })

    # Drop log_id (not needed for analysis)
    sleep_df = sleep_df.drop(columns=['log_id'])

    # Merge + sleep
    merged_df = pd.merge(
        merged_df,
        sleep_df,
        on=['Id', 'ActivityMinute'],
        how='left'  # left join because not every minute has sleep data
    )

    print(f"✅ Sleep merged! Total rows: {len(merged_df)}")

    

    # Rename columns
    merged_df = merged_df.rename(columns={
        'Id': 'user_id',
        'ActivityMinute': 'timestamp',
        'Steps': 'steps',
        'Calories': 'calories',
        'Intensity': 'intensity'
    })

    print(f"✅ Merged! Total rows: {len(merged_df)}")
    print("=" * 55)

    # ---- Tracking ----
    valid_count   = 0
    invalid_count = 0
    batch         = []
    batch_number  = 0
    total_failed  = 0
    BATCH_SIZE    = 100  # Firehose max = 500, we use 100 for safety

    # ---- Stream row by row ----
    for index, row in merged_df.iterrows():

        # Build event
        event = {
            "user_id"    : str(row['user_id']),
            "timestamp"  : str(row['timestamp']),
            "steps"      : int(row['steps']),
            "calories"   : float(row['calories']),
            "intensity"  : int(row['intensity']),
            "sleep_value": int(row['sleep_value']) if pd.notna(row.get('sleep_value')) else None,
            "event_type" : "minute_activity",
            "source"     : "fitbit_simulator",
            "ingest_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # Validate
        errors = validate_event(event)

        if errors:
            invalid_count += 1
            # Only print first 5 invalid to avoid spam
            if invalid_count <= 5:
                print(f"❌ Row {index} INVALID: {errors}")
        else:
            valid_count += 1
            batch.append(event)

        # When batch is full → send to Firehose
        if len(batch) >= BATCH_SIZE:
            batch_number += 1
            failed = send_to_firehose(batch, batch_number)
            total_failed += failed
            batch = []

            # Progress update every 10 batches
            if batch_number % 10 == 0:
                print(f"📊 Progress: {index+1}/{len(merged_df)} rows | "
                      f"Valid: {valid_count} | Invalid: {invalid_count}")

            time.sleep(0.05)  # small delay = simulate real-time

        # ⚠️ TEST MODE - remove this for full run
       

    # Send remaining batch
    if batch:
        batch_number += 1
        failed = send_to_firehose(batch, batch_number)
        total_failed += failed

    # ---- Final Summary ----
    print("\n" + "=" * 55)
    print("✅ SIMULATION COMPLETE")
    print(f"   Total rows processed : {valid_count + invalid_count}")
    print(f"   Valid events sent     : {valid_count}")
    print(f"   Invalid events skipped: {invalid_count}")
    print(f"   Total batches sent    : {batch_number}")
    print(f"   Firehose failures     : {total_failed}")
    print(f"\n   ⏳ Wait 60 seconds then check:")
    print(f"   s3://fitbit-bronze-layer/minute_data/")
    print("=" * 55)

# ================================
# RUN
# ================================
if __name__ == "__main__":
    simulate_stream()