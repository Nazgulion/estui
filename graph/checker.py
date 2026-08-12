from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class Verdict:
    finding: str
    verdicts: list[dict] = field(default_factory=list)
    passed: bool = False
    reason: str = ""


class CheckerNode:
    def __init__(
        self,
        checker_fn: Callable[[str], dict[str, Any]],
        min_passes: int = 2,
    ):
        self.checker_fn = checker_fn
        self.min_passes = min_passes

    def verify(self, finding: str, worker_context: str = "") -> Verdict:
        results = self.checker_fn(finding)
        checks = results.get("checks", [])
        passed_count = sum(1 for c in checks if c.get("passed", False))
        passed = passed_count >= self.min_passes

        reasons = []
        for c in checks:
            status = "PASS" if c.get("passed") else "FAIL"
            reasons.append(f"[{status}] {c.get('question', 'unknown')}: {c.get('reason', '')}")

        return Verdict(
            finding=finding,
            verdicts=checks,
            passed=passed,
            reason="\n".join(reasons),
        )

    @staticmethod
    def triple_check(finding: str, verify_fn: Callable[[str, str], bool]) -> dict[str, Any]:
        questions = [
            ("Is it correct?", "Does the claim actually hold up?"),
            ("Is it current?", "Is the source recent, not something stale?"),
            ("Is the source real?", "Does the link or reference resolve to what it claims?"),
        ]
        checks = []
        for question, description in questions:
            try:
                ok = verify_fn(finding, question)
            except Exception:
                ok = False
            checks.append({
                "question": question,
                "description": description,
                "passed": ok,
                "reason": "Verified" if ok else "Could not verify",
            })
        return {"checks": checks}
