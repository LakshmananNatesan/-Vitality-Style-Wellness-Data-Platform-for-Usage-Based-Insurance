import json
import boto3
import os
from datetime import datetime

# ─────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────
s3_client  = boto3.client('s3',  region_name='us-east-2')
sqs_client = boto3.client('sqs', region_name='us-east-2')
sns_client = boto3.client('sns', region_name='us-east-2')

# ─────────────────────────────────────
# ENV VARIABLES
# ─────────────────────────────────────
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

# ─────────────────────────────────────
# BUSINESS RULES
# ─────────────────────────────────────
def detect_event(row):
    total_steps   = float(row.get('total_steps',   0))
    anomaly_score = float(row.get('anomaly_score', 0))
    active_minutes = float(row.get('active_minutes', 0))

    if anomaly_score >= 0.8:
        return {
            "event_type": "FRAUD_DETECTED",
            "message":    f"Suspicious activity! Steps: {total_steps}",
            "priority":   "HIGH",
            "points":     0
        }
    elif total_steps >= 10000:
        return {
            "event_type": "WORKOUT_COMPLETED",
            "message":    f"Amazing! You hit {total_steps} steps today!",
            "priority":   "LOW",
            "points":     100
        }
    elif total_steps == 0:
        return {
            "event_type": "INACTIVITY_DETECTED",
            "message":    "No steps today — time to move!",
            "priority":   "MEDIUM",
            "points":     0
        }
    else:
        return None  # normal — no event

# ─────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────
def lambda_handler(event, context):
    print("Event Detection Lambda triggered!")

    # Step 1: Get Gold file from S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key    = event['Records'][0]['s3']['object']['key']

    print(f"New Gold file: s3://{bucket}/{key}")

    # Step 2: Read file
    s3_obj  = s3_client.get_object(Bucket=bucket, Key=key)
    content = s3_obj['Body'].read().decode('utf-8')
    rows    = content.strip().split('\n')

    print(f"Total rows: {len(rows)}")

    # Step 3: Process each row
    events_sent    = 0
    events_skipped = 0

    for row_str in rows:
        try:
            row = json.loads(row_str)

            user_id = row.get('user_id', 'UNKNOWN')
            day     = row.get('day',     0)

            # Detect business event
            detected = detect_event(row)

            if detected:
                # Build SQS message
                message = {
                    "user_id"   : user_id,
                    "day"       : day,
                    "event_type": detected['event_type'],
                    "message"   : detected['message'],
                    "priority"  : detected['priority'],
                    "points"    : detected['points'],
                    "timestamp" : datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                # Step 4: Send to SQS
                sqs_client.send_message(
                    QueueUrl    = SQS_QUEUE_URL,
                    MessageBody = json.dumps(message)
                )

                # Step 5: Send SNS alert for FRAUD
                if detected['event_type'] == 'FRAUD_DETECTED':
                    sns_client.publish(
                        TopicArn = SNS_TOPIC_ARN,
                        Subject  = "🚨 Fraud Detected!",
                        Message  = f"User: {user_id}\nDay: {day}\n{detected['message']}"
                    )

                events_sent += 1
                print(f"✅ {user_id} → {detected['event_type']}")

            else:
                events_skipped += 1

        except Exception as e:
            print(f"❌ Error processing row: {str(e)}")
            continue

    # Summary
    print(f"Events sent to SQS:  {events_sent}")
    print(f"Normal rows skipped: {events_skipped}")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'events_sent':    events_sent,
            'events_skipped': events_skipped
        })
    }
