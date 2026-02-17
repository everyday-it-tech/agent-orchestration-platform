import boto3
import json
import datetime
import os

# ===== CONFIG =====
REGION = "us-east-1"
ENV = "dev"

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/766464362927/agent-task-queue"
BUCKET_NAME = "rnd-pipeline-results-766464362927"

# ===== CLIENTS =====
sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# ===== RECEIVE MESSAGE =====
response = sqs.receive_message(
    QueueUrl=QUEUE_URL,
    MaxNumberOfMessages=1,
    WaitTimeSeconds=5
)

messages = response.get("Messages", [])

if not messages:
    print("No messages found.")
    exit()

message = messages[0]
receipt_handle = message["ReceiptHandle"]
message_id = message["MessageId"]

# Parse payload safely
try:
    body = json.loads(message["Body"])
except json.JSONDecodeError:
    body = {"raw": message["Body"]}

print("Processing task:")
print(json.dumps(body, indent=2))

# ===== SIMULATED RND EVALUATION =====
result = {
    "task_id": message_id,
    "environment": ENV,
    "received_at": datetime.datetime.utcnow().isoformat() + "Z",
    "evaluated_by": "rnd_worker",
    "status": "approved",
    "confidence_score": 0.87,
    "final_decision": "EXECUTE",
    "input": body
}

# ===== S3 KEY STRUCTURE =====
key = f"{ENV}/results/{message_id}.json"

# ===== UPLOAD TO S3 =====
s3.put_object(
    Bucket=BUCKET_NAME,
    Key=key,
    Body=json.dumps(result, indent=2),
    ContentType="application/json"
)

print(f"Result written to S3: s3://{BUCKET_NAME}/{key}")

# ===== DELETE MESSAGE =====
sqs.delete_message(
    QueueUrl=QUEUE_URL,
    ReceiptHandle=receipt_handle
)

print("Message deleted from queue.")
