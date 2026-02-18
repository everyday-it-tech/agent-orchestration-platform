import os


EVAL_MODE = os.getenv("EVAL_MODE", "auto").lower()
# auto | rnd | ops


class DeterministicEvaluationEngine:

    def evaluate(self, task_payload: dict) -> dict:

        task_type = (task_payload.get("task_type") or "").upper()
        payload = task_payload.get("payload", {}) or {}

        # -----------------------------
        # Mode Override Logic
        # -----------------------------
        if EVAL_MODE == "rnd":
            return self._evaluate_rnd(payload)

        if EVAL_MODE == "ops":
            return self._evaluate_ops(payload)

        # AUTO MODE
        if task_type == "LOG_SUGGESTION":
            return self._evaluate_ops(payload)

        return self._evaluate_rnd(payload)

    # ======================================================
    # RND SCORING MODEL (original deterministic_v2)
    # ======================================================

    def _evaluate_rnd(self, payload: dict) -> dict:

        idea_text = (
            payload.get("idea", "")
            or payload.get("title", "")
            or ""
        ).lower()

        priority = (
            payload.get("priority")
            or "medium"
        ).lower()

        feasibility = self._score_feasibility(idea_text)
        alignment = self._score_alignment(idea_text)
        complexity = self._score_complexity(idea_text)
        cost = self._score_cost(idea_text)

        weighted_score = (
            feasibility * 0.35 +
            alignment * 0.35 -
            complexity * 0.15 -
            cost * 0.15
        )

        if priority == "high":
            weighted_score += 0.1
        elif priority == "low":
            weighted_score -= 0.05

        weighted_score = max(0.0, min(1.0, weighted_score))
        decision = "EXECUTE" if weighted_score >= 0.6 else "REJECT"

        return {
            "final_decision": decision,
            "confidence_score": round(weighted_score, 3),
            "scoring": {
                "feasibility": feasibility,
                "alignment": alignment,
                "complexity_risk": complexity,
                "resource_cost": cost
            },
            "evaluation_model": "deterministic_rnd_v2"
        }

    # ======================================================
    # OPS SCORING MODEL (new)
    # ======================================================

    def _evaluate_ops(self, payload: dict) -> dict:

        severity = (payload.get("severity") or "low").lower()
        risk = float(payload.get("operational_risk", 0.3))
        confidence = float(payload.get("confidence", 0.5))

        score = confidence

        # Severity weighting
        if severity == "high":
            score += 0.4
        elif severity == "medium":
            score += 0.2
        else:
            score += 0.05

        # Risk penalty
        score -= (risk * 0.3)

        score = max(0.0, min(1.0, score))
        decision = "EXECUTE" if score >= 0.5 else "REJECT"

        return {
            "final_decision": decision,
            "confidence_score": round(score, 3),
            "scoring": {
                "severity": severity,
                "operational_risk": risk,
                "llm_confidence": confidence
            },
            "evaluation_model": "deterministic_ops_v1"
        }

    # ======================================================
    # ORIGINAL RND HELPERS
    # ======================================================

    def _score_feasibility(self, idea: str) -> float:
        if any(k in idea for k in ["sqs", "lambda", "api", "queue"]):
            return 0.9
        return 0.6

    def _score_alignment(self, idea: str) -> float:
        if any(k in idea for k in ["agent", "distributed", "orchestration"]):
            return 0.95
        return 0.7

    def _score_complexity(self, idea: str) -> float:
        if any(k in idea for k in ["ai", "ml", "blockchain", "crypto"]):
            return 0.7
        return 0.3

    def _score_cost(self, idea: str) -> float:
        if "distributed" in idea:
            return 0.6
        return 0.3
