import os
import json
import uuid
import datetime
import boto3
from openai import OpenAI

REGION = os.getenv("AWS_REGION", "us-east-1")

TASK_QUEUE_URL = os.getenv(
    "TASK_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/agent-task-queue",
)

LOG_FILE = os.getenv("LOG_FILE", "sample.log")

client = OpenAI()

sqs = boto3.client("sqs", region_name=REGION)


def utc_iso():
    return datetime.datetime.now(datetime.UTC).isoformat()


def new_id():
    return str(uuid.uuid4())


class LogIngestWorker:
    def __init__(self):
        print("[LogIngestWorker] Initialized")

    def run_once(self):
        logs = self.read_logs()
        if not logs:
            print("[LogIngestWorker] No logs found.")
            return

        suggestions = self.call_llm(logs)

        if not suggestions:
            print("[LogIngestWorker] No suggestions returned.")
            return

        for suggestion in suggestions:
            self.push_task(suggestion)

    def read_logs(self):
        if not os.path.exists(LOG_FILE):
            return None

        with open(LOG_FILE, "r", encoding="utf-8") as f:
            return f.read()

    def call_llm(self, logs: str):
        system_prompt = """
You are a systems diagnostics assistant.

Analyze the provided logs and return structured JSON ONLY.

Return format:
{
  "ideas": [
    {
      "title": "...",
      "description": "...",
      "severity": "low | medium | high",
      "recommended_action": "..."
    }
  ]
}
        """

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": logs}
            ],
            temperature=0
        )

        raw = response.choices[0].message.content

        try:
            parsed = json.loads(raw)
            return parsed.get("ideas", [])
        except Exception as e:
            print("[LogIngestWorker] Failed to parse LLM output:", e)
            print(raw)
            return None

    def push_task(self, idea):
        trace_id = new_id()
        task_id = new_id()

        payload = {
            "task_id": task_id,
            "trace_id": trace_id,
            "task_type": "LOG_SUGGESTION",
            "agent": "log_ingest_worker",
            "payload": idea,
            "created_at": utc_iso()
        }

        sqs.send_message(
            QueueUrl=TASK_QUEUE_URL,
            MessageBody=json.dumps(payload, indent=2),
            MessageAttributes={
                "trace_id": {"DataType": "String", "StringValue": trace_id},
                "task_id": {"DataType": "String", "StringValue": task_id},
                "stage": {"DataType": "String", "StringValue": "evaluation"},
            },
        )

        print(f"[LogIngestWorker] Sent suggestion task_id={task_id}")


if __name__ == "__main__":
    LogIngestWorker().run_once()
