import os
import json
import uuid
import datetime
import boto3
from openai import OpenAI
import subprocess
import platform
import hashlib
from typing import Optional, Dict, Any


# -------------------------
# SUPPRESSION CONFIG
# -------------------------

SUPPRESSION_WINDOW_MINUTES = int(os.getenv("SUPPRESSION_WINDOW_MINUTES", "60"))
PENDING_SUPPRESSION_MINUTES = int(os.getenv("PENDING_SUPPRESSION_MINUTES", "30"))
EVALUATION_PREFIX = os.getenv("EVALUATION_PREFIX", "evaluations/")
EXECUTION_PREFIX = os.getenv("EXECUTION_PREFIX", "executions/")


# -------------------------
# ENV CONFIG
# -------------------------

REGION = os.getenv("AWS_REGION", "us-east-1")
TASK_QUEUE_URL = os.getenv(
    "TASK_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/agent-task-queue",
)

LOG_MODE = os.getenv("LOG_MODE", "auto")
LOG_FILE = os.getenv("LOG_FILE")
LOG_COMMAND = os.getenv("LOG_COMMAND")

MAX_LOG_CHARS = int(os.getenv("MAX_LOG_CHARS", "12000"))

BUCKET_NAME = os.getenv(
    "BUCKET_NAME",
    "rnd-pipeline-results-766464362927",
)

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

client = OpenAI()
sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# -------------------------
# HELPERS
# -------------------------

def utc_iso() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _safe_json_loads(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _stable_text(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _idea_fingerprint(idea: Dict[str, Any]) -> str:
    title = _stable_text(idea.get("title")).lower()
    action = _stable_text(idea.get("recommended_action")).lower()
    desc = _stable_text(idea.get("description")).lower()
    base = f"{title}\n{action}\n{desc}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()


# -------------------------
# WORKER
# -------------------------

class LogIngestWorker:

    def __init__(self) -> None:
        print("[LogIngestWorker] Initialized")
        print(f"[LogIngestWorker] LOG_MODE={LOG_MODE}")

        self.completed_fingerprints = self._build_recent_completed_fingerprints()
        self.pending_fingerprints = self._build_recent_pending_fingerprints()


    # -------------------------
    # MAIN
    # -------------------------

    def run_once(self) -> None:
        logs = self.read_logs()
        if not logs:
            print("[LogIngestWorker] No logs found.")
            return

        logs = logs[:MAX_LOG_CHARS]

        suggestions = self.call_llm(logs)
        if not suggestions:
            print("[LogIngestWorker] No suggestions returned.")
            return

        pushed = 0
        skipped = 0

        for suggestion in suggestions:
            if not isinstance(suggestion, dict):
                skipped += 1
                continue

            if self.is_duplicate(suggestion):
                skipped += 1
                print(f"[LogIngestWorker] Suppressed duplicate: {suggestion.get('title')}")
                continue

            self.push_task(suggestion)
            pushed += 1

        print(f"[LogIngestWorker] Done. pushed={pushed} skipped={skipped}")


    # -------------------------
    # SUPPRESSION LOGIC
    # -------------------------

    def _build_recent_completed_fingerprints(self) -> set:
        done = set()
        cutoff = _utcnow() - datetime.timedelta(minutes=SUPPRESSION_WINDOW_MINUTES)

        try:
            resp = s3.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=EXECUTION_PREFIX,
                MaxKeys=50,
            )

            for obj in resp.get("Contents", []) or []:
                key = obj.get("Key")
                if not key:
                    continue

                file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                raw = file_obj["Body"].read().decode("utf-8", errors="ignore")
                data = _safe_json_loads(raw)

                status = _stable_text(data.get("status")).upper()
                executed_at = data.get("executed_at")

                if status != "COMPLETED":
                    continue

                if executed_at:
                    try:
                        dt = datetime.datetime.fromisoformat(executed_at)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=datetime.UTC)
                        if dt < cutoff:
                            continue
                    except Exception:
                        continue

                fp = data.get("fingerprint")
                if fp:
                    done.add(fp)

        except Exception as e:
            print("Completed suppression load failed:", e)

        return done


    def _build_recent_pending_fingerprints(self) -> set:
        pending = set()
        cutoff = _utcnow() - datetime.timedelta(minutes=PENDING_SUPPRESSION_MINUTES)

        try:
            resp = s3.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=EVALUATION_PREFIX,
                MaxKeys=50,
            )

            for obj in resp.get("Contents", []) or []:
                lm = obj.get("LastModified")
                if lm and lm < cutoff:
                    continue

                key = obj.get("Key")
                if not key:
                    continue

                file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                raw = file_obj["Body"].read().decode("utf-8", errors="ignore")
                data = _safe_json_loads(raw)

                original = data.get("original_payload") or {}
                if isinstance(original, dict):
                    payload = original.get("payload") or {}
                    fp = payload.get("fingerprint")
                    if fp:
                        pending.add(fp)

        except Exception as e:
            print("Pending suppression load failed:", e)

        return pending


    def is_duplicate(self, suggestion: Dict[str, Any]) -> bool:
        fp = _idea_fingerprint(suggestion)

        if fp in self.completed_fingerprints:
            return True

        if fp in self.pending_fingerprints:
            return True

        return False


    # -------------------------
    # LOG READING
    # -------------------------

    def read_logs(self) -> Optional[str]:
        if LOG_MODE == "auto":
            return self._auto_detect_logs()

        if LOG_MODE == "file" and LOG_FILE:
            return self._read_file(LOG_FILE)

        if LOG_MODE == "command" and LOG_COMMAND:
            return self._run_command(LOG_COMMAND)

        return None


    def _auto_detect_logs(self) -> Optional[str]:
        system = platform.system()

        if system == "Windows":
            return self._windows_event_log()
        if system == "Linux":
            return self._linux_journal()
        if system == "Darwin":
            return self._mac_logs()

        return None


    def _windows_event_log(self):
        result = subprocess.run(
            ["powershell", "-Command", "Get-EventLog -LogName System -Newest 200"],
            capture_output=True,
            text=True,
        )
        return result.stdout or result.stderr or None


    def _linux_journal(self):
        result = subprocess.run(
            ["journalctl", "-n", "200", "--no-pager"],
            capture_output=True,
            text=True,
        )
        return result.stdout or result.stderr or None


    def _mac_logs(self):
        result = subprocess.run(
            ["log", "show", "--style", "syslog", "--last", "1h"],
            capture_output=True,
            text=True,
        )
        return result.stdout or result.stderr or None


    def _read_file(self, path: str):
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()


    def _run_command(self, cmd: str):
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        return result.stdout or result.stderr or None


    # -------------------------
    # LLM
    # -------------------------

    def call_llm(self, logs: str):
        system_prompt = """
    You are a senior systems reliability engineer.

    Analyze operating system logs and return STRICT JSON only.

    Return format:

    {
    "ideas": [
        {
        "title": "...",
        "description": "...",
        "severity": "low | medium | high",
        "operational_risk": 0.0,
        "confidence": 0.0,
        "recommended_action": "..."
        }
    ]
    }

    If nothing actionable exists, return:
    { "ideas": [] }
    """

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": logs},
            ],
            temperature=0,
        )

        raw = response.choices[0].message.content or ""
        parsed = _safe_json_loads(raw)

        ideas = parsed.get("ideas")

        if not isinstance(ideas, list):
            print("LLM returned unexpected format:")
            print(raw)
            return None

        return ideas


    # -------------------------
    # QUEUE
    # -------------------------

    def push_task(self, idea: Dict[str, Any]):
        trace_id = new_id()
        task_id = new_id()

        idea = dict(idea)
        idea["fingerprint"] = _idea_fingerprint(idea)

        payload = {
            "task_id": task_id,
            "trace_id": trace_id,
            "task_type": "LOG_SUGGESTION",
            "agent": "log_ingest_worker",
            "payload": idea,
            "created_at": utc_iso(),
        }

        sqs.send_message(
            QueueUrl=TASK_QUEUE_URL,
            MessageBody=json.dumps(payload),
        )

        print(f"[LogIngestWorker] Sent suggestion task_id={task_id}")


if __name__ == "__main__":
    LogIngestWorker().run_once()
