import datetime
from typing import Dict


def utc_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


class PolicyEngine:
    """
    Deterministic policy gate between evaluation and execution.

    Responsible for:
        - Enforcing trust tiers
        - Deciding HITL vs AUTO_EXECUTE
        - Applying risk thresholds
        - Making autonomy explicit and auditable
    """

    def __init__(self) -> None:
        # Thresholds can later move to config / env
        self.auto_execute_threshold = 0.85
        self.max_allowed_complexity_risk = 0.4
        self.max_allowed_resource_cost = 0.4

        print("[PolicyEngine] Initialized")

    def evaluate(self, evaluation: Dict) -> Dict:
        """
        Accepts evaluation payload and returns policy decision.
        """

        confidence = evaluation.get("confidence_score", 0.0)
        breakdown = evaluation.get("scoring_breakdown", {})

        complexity_risk = breakdown.get("complexity_risk", 1.0)
        resource_cost = breakdown.get("resource_cost", 1.0)
        final_decision = evaluation.get("final_decision")

        reasoning = []
        mode = "REQUIRE_HITL"

        # --- Hard rejection ---
        if final_decision == "REJECT":
            reasoning.append("Evaluation engine rejected idea.")
            return self._result(
                mode="REJECT",
                reasoning=reasoning,
                evaluation=evaluation,
            )

        # --- Auto execution eligibility ---
        if (
            confidence >= self.auto_execute_threshold
            and complexity_risk <= self.max_allowed_complexity_risk
            and resource_cost <= self.max_allowed_resource_cost
        ):
            mode = "AUTO_EXECUTE"
            reasoning.append("Meets confidence and risk thresholds.")
        else:
            mode = "REQUIRE_HITL"
            reasoning.append("Does not meet auto execution thresholds.")

        return self._result(
            mode=mode,
            reasoning=reasoning,
            evaluation=evaluation,
        )

    def _result(self, mode: str, reasoning: list, evaluation: Dict) -> Dict:
        """
        Standardized policy decision structure.
        """

        return {
            "policy_mode": mode,
            "policy_timestamp": utc_iso(),
            "policy_reasoning": reasoning,
            "policy_version": "v1_deterministic",
            "evaluation_snapshot": evaluation,
        }
