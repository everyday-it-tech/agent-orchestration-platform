import os
import json
import uuid
import datetime
import boto3

REGION = os.getenv("AWS_REGION", "us-east-1")

EXEC_QUEUE_URL = os.getenv(
    "EXEC_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/execution-queue",
)

BUCKET_NAME = os.getenv(
    "BUCKET_NAME",
    "rnd-pipeline-results-766464362927",
)

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def utc_iso() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


def safe_json_loads(raw: str):
    try:
        return json.loads(raw)
    except Exception:
        return {"raw_body": raw}


class ExecWorker:
    def __init__(self) -> None:
        print("[ExecWorker] Initialized")

    def run_once(self) -> None:
        resp = sqs.receive_message(
            QueueUrl=EXEC_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )

        messages = resp.get("Messages", [])
        if not messages:
            print("[ExecWorker] No messages found.")
            return

        msg = messages[0]
        receipt = msg["ReceiptHandle"]

        body = safe_json_loads(msg.get("Body", ""))
        trace_id = body.get("trace_id") or self._trace_from_attrs(msg) or "missing-trace"
        task_id = body.get("task_id") or "missing-task"

        print(f"[ExecWorker] [trace={trace_id}] Executing task_id={task_id}")

        result = self.execute_task(body)
        result["trace_id"] = trace_id
        result["task_id"] = task_id
        result["executed_at"] = utc_iso()
        result["executed_by"] = "exec_worker"
        result["exec_id"] = new_id()

        key = f"executions/{task_id}.json"
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(result, indent=2),
            ContentType="application/json",
        )
        print(f"[ExecWorker] [trace={trace_id}] Wrote execution to s3://{BUCKET_NAME}/{key}")

        sqs.delete_message(QueueUrl=EXEC_QUEUE_URL, ReceiptHandle=receipt)
        print(f"[ExecWorker] [trace={trace_id}] Execution message deleted")

    def execute_task(self, envelope: dict) -> dict:
        """
        Only execute if explicitly approved by HITL.
        """

        decision = envelope.get("decision")
        evaluation = envelope.get("evaluation", {})

        if decision != "APPROVE":
            return {
                "status": "BLOCKED",
                "notes": "Execution blocked. Missing explicit HITL approval.",
                "received_packet": envelope,
            }

        # At this stage, this is an approved action
        return {
            "status": "COMPLETED",
            "notes": "Simulated execution complete (HITL approved).",
            "approved_by": envelope.get("decided_by"),
            "evaluation": evaluation,
        }



    def _trace_from_attrs(self, msg: dict) -> str | None:
        attrs = msg.get("MessageAttributes") or {}
        trace = attrs.get("trace_id") or {}
        return trace.get("StringValue")


if __name__ == "__main__":
    ExecWorker().run_once()
