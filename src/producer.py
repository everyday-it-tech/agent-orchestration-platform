import os
import json
import uuid
import datetime
import boto3

REGION = os.getenv("AWS_REGION", "us-east-1")
TASK_QUEUE_URL = os.getenv(
    "TASK_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/agent-task-queue",
)

sqs = boto3.client("sqs", region_name=REGION)


def utc_iso() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


def main() -> None:
    task_id = new_id()
    trace_id = new_id()

    task = {
        "task_id": task_id,
        "trace_id": trace_id,
        "created_at": utc_iso(),
        "task_type": "RND_ANALYSIS",
        "agent": "agent_creator",
        "payload": {
            "idea": "Build distributed agent bus using SQS",
            "priority": "high",
        },
    }

    body = json.dumps(task, indent=2)

    resp = sqs.send_message(
        QueueUrl=TASK_QUEUE_URL,
        MessageBody=body,
        MessageAttributes={
            "trace_id": {"DataType": "String", "StringValue": trace_id},
            "task_id": {"DataType": "String", "StringValue": task_id},
            "task_type": {"DataType": "String", "StringValue": task["task_type"]},
        },
    )

    print(f"[Producer] [trace={trace_id}] Sent task_id={task_id} MessageId={resp.get('MessageId')}")


if __name__ == "__main__":
    main()
