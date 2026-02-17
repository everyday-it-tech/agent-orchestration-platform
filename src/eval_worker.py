from evaluation_engine import DeterministicEvaluationEngine

import os
import json
import uuid
import datetime
import time
import boto3
import botocore.exceptions


REGION = os.getenv("AWS_REGION", "us-east-1")

TASK_QUEUE_URL = os.getenv(
    "TASK_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/agent-task-queue",
)

HITL_QUEUE_URL = os.getenv(
    "HITL_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/hitl-approval-queue",
)

BUCKET_NAME = os.getenv(
    "BUCKET_NAME",
    "rnd-pipeline-results-766464362927",
)

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))


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


class EvalWorker:
    def __init__(self) -> None:
        self.engine = DeterministicEvaluationEngine()
        print("[EvalWorker] Initialized")

    def run_forever(self) -> None:
        print("[EvalWorker] Loop started")
        while True:
            try:
                self.run_once()
            except Exception as e:
                print("[EvalWorker] ERROR:", repr(e))
            time.sleep(POLL_INTERVAL)

    def run_once(self) -> None:
        resp = sqs.receive_message(
            QueueUrl=TASK_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )

        messages = resp.get("Messages", [])
        if not messages:
            print("[EvalWorker] No messages")
            return

        msg = messages[0]
        receipt = msg["ReceiptHandle"]

        body = safe_json_loads(msg.get("Body", ""))

        trace_id = (
            body.get("trace_id")
            or self._trace_from_attrs(msg)
            or "missing-trace"
        )
        task_id = body.get("task_id") or new_id()

        print(f"[EvalWorker] [trace={trace_id}] Evaluating task_id={task_id}")

        try:
            engine_result = self.engine.evaluate(body)
        except Exception as e:
            print("[EvalWorker] Engine failure:", repr(e))
            return

        evaluation = {
            "eval_id": new_id(),
            "task_id": task_id,
            "trace_id": trace_id,
            "evaluated_at": utc_iso(),
            "evaluated_by": "eval_worker",
            **engine_result,
        }

        self._write_s3_json(
            key=f"evaluations/{task_id}.json",
            payload=evaluation,
            trace_id=trace_id,
        )

        final_decision = engine_result.get("final_decision", "REJECT")

        if final_decision == "EXECUTE":
            approval_packet = {
                "task_id": task_id,
                "trace_id": trace_id,
                "stage": "hitl",
                "evaluation": evaluation,
                "original_payload": body,
                "created_at": utc_iso(),
            }

            self._send_hitl(task_id, trace_id, approval_packet)
        else:
            reject_packet = {
                "task_id": task_id,
                "trace_id": trace_id,
                "stage": "rejected",
                "evaluation": evaluation,
                "original_payload": body,
                "created_at": utc_iso(),
            }
            self._write_s3_json(
                key=f"rejections/{task_id}.json",
                payload=reject_packet,
                trace_id=trace_id,
            )
            print(f"[EvalWorker] [trace={trace_id}] Rejected task_id={task_id}")

        sqs.delete_message(QueueUrl=TASK_QUEUE_URL, ReceiptHandle=receipt)
        print(f"[EvalWorker] [trace={trace_id}] Deleted original message")

    def _send_hitl(self, task_id: str, trace_id: str, packet: dict) -> None:
        try:
            sqs.send_message(
                QueueUrl=HITL_QUEUE_URL,
                MessageBody=json.dumps(packet),
                MessageAttributes={
                    "trace_id": {"DataType": "String", "StringValue": trace_id},
                    "task_id": {"DataType": "String", "StringValue": task_id},
                    "stage": {"DataType": "String", "StringValue": "hitl"},
                },
            )
            print(f"[EvalWorker] [trace={trace_id}] Sent task_id={task_id} to HITL queue")
        except botocore.exceptions.BotoCoreError as e:
            print("[EvalWorker] HITL send error:", repr(e))

    def _write_s3_json(self, key: str, payload: dict, trace_id: str) -> None:
        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=json.dumps(payload, indent=2),
                ContentType="application/json",
            )
            print(f"[EvalWorker] [trace={trace_id}] Wrote s3://{BUCKET_NAME}/{key}")
        except botocore.exceptions.BotoCoreError as e:
            print("[EvalWorker] S3 write error:", repr(e))

    def _trace_from_attrs(self, msg: dict) -> str | None:
        attrs = msg.get("MessageAttributes") or {}
        trace = attrs.get("trace_id") or {}
        return trace.get("StringValue")


if __name__ == "__main__":
    EvalWorker().run_forever()
