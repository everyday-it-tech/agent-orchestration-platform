import os
import json
import uuid
import datetime
import boto3
import hashlib
from typing import Any, Dict, Optional


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


def idea_fingerprint(idea: dict) -> str:
    """
    Fallback only. Fingerprints should be generated ONCE upstream and propagated unchanged.
    """
    title = str(idea.get("title", "")).lower().strip()
    action = str(idea.get("recommended_action", "")).lower().strip()
    desc = str(idea.get("description", "")).lower().strip()

    base = f"{title}\n{action}\n{desc}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()


def _deep_get(d: Any, path: list[str], default=None):
    cur = d
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


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

        # --------------------------
        # Extract original envelope and idea payload safely
        # --------------------------
        # The HITL decision packet shape is:
        # body = { decision, decided_by, original_packet: { original_payload: { payload: { ...idea... } } } }
        original_packet = body.get("original_packet") or {}

        # Keep BOTH names because your pipeline has used both at different points.
        original_envelope = (
            original_packet.get("original_payload")
            or original_packet.get("payload")
            or {}
        )

        # For LOG_SUGGESTION, the actual idea dict is at original_envelope["payload"]
        idea_payload = {}
        if isinstance(original_envelope, dict):
            idea_payload = original_envelope.get("payload") or {}

        # --------------------------
        # Fingerprint: propagate, do not mutate
        # --------------------------
        fingerprint = None

        if isinstance(idea_payload, dict):
            fingerprint = idea_payload.get("fingerprint")

        # Fallback only, in case older records exist without fingerprint
        if not fingerprint and isinstance(idea_payload, dict):
            fingerprint = idea_fingerprint(idea_payload)

        # --------------------------
        # Build execution record
        # --------------------------
        execution_record: Dict[str, Any] = {
            "exec_id": new_id(),
            "task_id": task_id,
            "trace_id": trace_id,
            "status": result.get("status"),
            "notes": result.get("notes"),
            "approved_by": result.get("approved_by"),
            "executed_at": utc_iso(),
            "executed_by": "exec_worker",
            "evaluation": result.get("evaluation") or {},
            "fingerprint": fingerprint,
            # Preserve full original envelope for audit, unchanged
            "original_idea": original_envelope,
            # Convenience fields for future analytics and debugging
            "idea_payload": idea_payload,
        }

        key = f"executions/{task_id}.json"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(execution_record, indent=2),
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
        evaluation = envelope.get("evaluation", {}) or {}

        if decision != "APPROVE":
            return {
                "status": "BLOCKED",
                "notes": "Execution blocked. Missing explicit HITL approval.",
                "evaluation": evaluation,
            }

        return {
            "status": "COMPLETED",
            "notes": "Simulated execution complete (HITL approved).",
            "approved_by": envelope.get("decided_by"),
            "evaluation": evaluation,
        }

    def _trace_from_attrs(self, msg: dict) -> Optional[str]:
        attrs = msg.get("MessageAttributes") or {}
        trace = attrs.get("trace_id") or {}
        return trace.get("StringValue")


if __name__ == "__main__":
    ExecWorker().run_once()
