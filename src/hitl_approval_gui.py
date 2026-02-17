import os
import json
import datetime
import tkinter as tk
from tkinter import ttk, messagebox

import boto3
import botocore.exceptions


REGION = os.getenv("AWS_REGION", "us-east-1")

HITL_QUEUE_URL = os.getenv(
    "HITL_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/hitl-approval-queue",
)

EXEC_QUEUE_URL = os.getenv(
    "EXEC_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/766464362927/execution-queue",
)

BUCKET_NAME = os.getenv(
    "BUCKET_NAME",
    "rnd-pipeline-results-766464362927",
)

POLL_MS = int(os.getenv("HITL_POLL_MS", "2500"))

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def utc_iso() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat()


class HITLApprovalGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("HITL Approval Console")
        self.geometry("1100x650")

        self.messages = []
        self.selected_index = None

        self._build_ui()
        self.after(500, self.poll_queue)

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Pending Approvals", font=("Segoe UI", 14, "bold")).pack(side="left")
        ttk.Button(top, text="Refresh", command=self.poll_queue).pack(side="right")

        main = ttk.Frame(self, padding=10)
        main.pack(fill="both", expand=True)

        left = ttk.Frame(main)
        left.pack(side="left", fill="y")

        self.listbox = tk.Listbox(left, width=55, height=28)
        self.listbox.pack(side="left", fill="y")
        self.listbox.bind("<<ListboxSelect>>", self.on_select)

        scrollbar = ttk.Scrollbar(left, orient="vertical", command=self.listbox.yview)
        scrollbar.pack(side="right", fill="y")
        self.listbox.configure(yscrollcommand=scrollbar.set)

        right = ttk.Frame(main)
        right.pack(side="right", fill="both", expand=True)

        ttk.Label(right, text="Details", font=("Segoe UI", 12, "bold")).pack(anchor="w")
        self.text = tk.Text(right, wrap="word")
        self.text.pack(fill="both", expand=True)

        buttons = ttk.Frame(self, padding=10)
        buttons.pack(fill="x")

        ttk.Button(buttons, text="Approve", command=self.approve_selected).pack(side="right", padx=5)
        ttk.Button(buttons, text="Reject", command=self.reject_selected).pack(side="right", padx=5)

        ttk.Label(
            buttons,
            text="Worker execution is blocked unless you explicitly approve.",
        ).pack(side="left")

    def poll_queue(self):
        try:
            resp = sqs.receive_message(
                QueueUrl=HITL_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
                MessageAttributeNames=["All"],
                VisibilityTimeout=20,
            )
        except botocore.exceptions.BotoCoreError as e:
            self.after(POLL_MS, self.poll_queue)
            return

        msgs = resp.get("Messages", [])
        if msgs:
            for m in msgs:
                body = m.get("Body", "")
                payload = self._safe_json(body)
                task_id = payload.get("task_id", "unknown-task")
                trace_id = payload.get("trace_id", "unknown-trace")
                title = self._extract_title(payload)

                self.messages.append({
                    "raw": m,
                    "payload": payload,
                    "task_id": task_id,
                    "trace_id": trace_id,
                    "title": title,
                })

                self.listbox.insert("end", f"{task_id}   {title}")

        self.after(POLL_MS, self.poll_queue)

    def on_select(self, _evt=None):
        sel = self.listbox.curselection()
        if not sel:
            return
        idx = int(sel[0])
        self.selected_index = idx

        item = self.messages[idx]
        self.text.delete("1.0", "end")
        self.text.insert("1.0", json.dumps(item["payload"], indent=2))

    def approve_selected(self):
        item = self._selected_item()
        if not item:
            return

        if not messagebox.askyesno("Approve", "Approve and forward to execution queue?"):
            return

        payload = item["payload"]
        task_id = item["task_id"]
        trace_id = item["trace_id"]

        decision = {
            "task_id": task_id,
            "trace_id": trace_id,
            "decision": "APPROVE",
            "decided_at": utc_iso(),
            "decided_by": "hitl_gui",
            "original_packet": payload,
        }

        try:
            sqs.send_message(
                QueueUrl=EXEC_QUEUE_URL,
                MessageBody=json.dumps(decision),
                MessageAttributes={
                    "trace_id": {"DataType": "String", "StringValue": trace_id},
                    "task_id": {"DataType": "String", "StringValue": task_id},
                    "stage": {"DataType": "String", "StringValue": "execution"},
                    "hitl": {"DataType": "String", "StringValue": "approved"},
                },
            )
        except botocore.exceptions.BotoCoreError as e:
            messagebox.showerror("Error", f"Failed to forward to execution.\n{e}")
            return

        self._archive_decision(task_id, decision, prefix="hitl_approvals")
        self._delete_hitl_message(item)

        messagebox.showinfo("Approved", "Forwarded to execution queue.")
        self._remove_from_ui(item)

    def reject_selected(self):
        item = self._selected_item()
        if not item:
            return

        if not messagebox.askyesno("Reject", "Reject and archive decision?"):
            return

        payload = item["payload"]
        task_id = item["task_id"]
        trace_id = item["trace_id"]

        decision = {
            "task_id": task_id,
            "trace_id": trace_id,
            "decision": "REJECT",
            "decided_at": utc_iso(),
            "decided_by": "hitl_gui",
             "evaluation": payload.get("evaluation"),
            "original_packet": payload,
        }

        self._archive_decision(task_id, decision, prefix="hitl_rejections")
        self._delete_hitl_message(item)

        messagebox.showinfo("Rejected", "Rejected and archived.")
        self._remove_from_ui(item)

    def _archive_decision(self, task_id: str, decision: dict, prefix: str):
        key = f"{prefix}/{task_id}.json"
        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=json.dumps(decision, indent=2),
                ContentType="application/json",
            )
        except botocore.exceptions.BotoCoreError:
            pass

    def _delete_hitl_message(self, item: dict):
        raw = item["raw"]
        receipt = raw.get("ReceiptHandle")
        if not receipt:
            return
        try:
            sqs.delete_message(QueueUrl=HITL_QUEUE_URL, ReceiptHandle=receipt)
        except botocore.exceptions.BotoCoreError:
            pass

    def _remove_from_ui(self, item: dict):
        idx = self.messages.index(item)
        self.messages.pop(idx)
        self.listbox.delete(idx)
        self.text.delete("1.0", "end")
        self.selected_index = None

    def _selected_item(self):
        if self.selected_index is None:
            messagebox.showwarning("Select", "Select an item first.")
            return None
        if self.selected_index >= len(self.messages):
            return None
        return self.messages[self.selected_index]

    def _safe_json(self, raw: str):
        try:
            return json.loads(raw)
        except Exception:
            return {"raw_body": raw}

    def _extract_title(self, payload: dict) -> str:
        p = payload.get("original_payload", payload)
        inner = p.get("payload", {}) if isinstance(p, dict) else {}
        title = inner.get("title") or inner.get("idea") or payload.get("stage") or "pending"
        return str(title)[:60]


if __name__ == "__main__":
    app = HITLApprovalGUI()
    app.mainloop()
