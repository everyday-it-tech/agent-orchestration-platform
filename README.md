Agent Orchestration Platform

A distributed, asynchronous agent orchestration system built using AWS SQS and S3.

This project implements a multi stage pipeline that evaluates, approves, and executes tasks using decoupled workers and persistent artifact storage.

Overview

The system models a production style workflow with:

Asynchronous message passing via SQS

Deterministic evaluation and scoring

Human in the Loop approval gating

Execution workers consuming approval envelopes

Immutable execution artifacts stored in S3

Traceable task lifecycle with correlation IDs

The architecture separates control, evaluation, approval, and execution into independent services.

Architecture

High level pipeline flow:

Producer
→ Evaluation Queue (SQS)
→ Eval Worker
→ Evaluation Artifact (S3)
→ HITL Approval
→ Execution Queue (SQS)
→ Exec Worker
→ Execution Artifact (S3)

Core Concepts

Decoupled Workers
Services communicate only through SQS. No direct service to service calls.

Artifact Persistence
All evaluation, approval, and execution decisions are written to S3 for auditability.

Human in the Loop Gating
Execution requires explicit approval before the exec worker processes the task.

Trace Propagation
Each task includes a trace_id for lifecycle tracking across services.

Project Structure
.
│   docker-compose.yml
│   dockerfile
│   requirements.txt
│   README.md
│
└───src
    │   producer.py
    │   evaluation_engine.py
    │   eval_worker.py
    │   exec_worker.py
    │   hitl_approval_gui.py
    │   policy_engine.py
    │   log_ingest_worker.py
    │   worker.py

Key Components
producer.py

Creates task messages and publishes them to the evaluation queue.

evaluation_engine.py

Implements deterministic scoring logic for task feasibility, alignment, risk, and cost.

eval_worker.py

Consumes evaluation messages from SQS, scores them, and writes structured evaluation artifacts to S3.

hitl_approval_gui.py

Provides human approval interface. Approved tasks are forwarded to the execution queue.

exec_worker.py

Consumes approved execution envelopes from SQS and writes execution results to S3.

policy_engine.py

Applies policy constraints or gating rules prior to execution.

log_ingest_worker.py

Processes structured logs and forwards them through the pipeline.

Design Decisions
Why SQS?

Enables horizontal scaling of workers.

Provides natural decoupling between services.

Allows retry and failure isolation.

Why S3 for Artifacts?

Durable and immutable storage.

Enables auditability and trace reconstruction.

Separates execution logic from state persistence.

Why Explicit HITL Approval?

Prevents automatic execution of potentially unsafe tasks.

Mirrors enterprise governance patterns.

Enables hybrid AI + human workflows.

Why Deterministic Scoring?

Transparent and reproducible evaluation decisions.

No hidden probabilistic behavior.

Easier to reason about during debugging.

Failure Handling

Workers poll with long polling to reduce empty receives.

Execution is gated on explicit approval.

Artifacts are written before message deletion.

Messages are deleted only after successful processing.

Potential enhancements:

Idempotency keys for duplicate prevention.

Dead letter queues.

Execution replay support.

Per task execution history folders.

Running Locally

Configure AWS credentials.

Set environment variables:

AWS_REGION
EXEC_QUEUE_URL
EVAL_QUEUE_URL
BUCKET_NAME


Run services:

python src/eval_worker.py
python src/exec_worker.py
python src/producer.py


Docker support is included via:

docker build -t agent-orchestrator .
docker run agent-orchestrator

What This Demonstrates

Distributed systems fundamentals

Asynchronous worker architecture

Human approval workflows

Event driven design

Durable artifact storage

Trace based debugging

AWS service integration

Future Enhancements

Idempotent execution protection

Per task execution history

Lambda based scaling

Structured metrics and monitoring

Policy based auto approval thresholds

Multi tenant support