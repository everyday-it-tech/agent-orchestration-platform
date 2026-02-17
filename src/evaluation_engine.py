class DeterministicEvaluationEngine:

    def evaluate(self, task_payload: dict) -> dict:
        idea_text = (
            task_payload.get("payload", {}).get("idea", "")
            or ""
        ).lower()

        priority = (
            task_payload.get("payload", {}).get("priority")
            or task_payload.get("priority")
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
            "evaluation_model": "deterministic_v2"
        }

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
