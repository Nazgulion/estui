from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class CheckCriteria:
    description: str
    test: Callable[[str], bool]
    fail_message: str = ""


@dataclass
class CheckResult:
    passed: bool
    criteria_results: list[dict] = field(default_factory=list)
    overall_score: float = 0.0
    feedback: str = ""

    @property
    def all_passed(self) -> bool:
        return self.passed


class SelfChecker:
    def __init__(
        self,
        criteria: list[CheckCriteria],
        threshold: float = 1.0,
    ):
        self.criteria = criteria
        self.threshold = threshold

    def check(self, output: str) -> CheckResult:
        results = []
        passed_count = 0

        for criterion in self.criteria:
            try:
                ok = criterion.test(output)
            except Exception as e:
                ok = False

            results.append({
                "criterion": criterion.description,
                "passed": ok,
                "message": "" if ok else criterion.fail_message,
            })
            if ok:
                passed_count += 1

        score = passed_count / len(self.criteria) if self.criteria else 0.0
        passed = score >= self.threshold

        feedback_lines = []
        for r in results:
            status = "PASS" if r["passed"] else "FAIL"
            feedback_lines.append(f"  [{status}] {r['criterion']}")
            if not r["passed"] and r["message"]:
                feedback_lines.append(f"         {r['message']}")

        return CheckResult(
            passed=passed,
            criteria_results=results,
            overall_score=score,
            feedback="\n".join(feedback_lines),
        )

    @staticmethod
    def create_from_rubric(rubric: dict[str, Callable[[str], bool]], threshold: float = 1.0) -> "SelfChecker":
        criteria = [
            CheckCriteria(description=desc, test=test_fn, fail_message=f"Failed: {desc}")
            for desc, test_fn in rubric.items()
        ]
        return SelfChecker(criteria, threshold)


def length_check(min_chars: int = 10) -> Callable[[str], bool]:
    return lambda output: len(output.strip()) >= min_chars


def contains_check(keywords: list[str]) -> Callable[[str], bool]:
    def _check(output: str) -> bool:
        lowered = output.lower()
        return all(kw.lower() in lowered for kw in keywords)
    return _check


def not_contains_check(forbidden: list[str]) -> Callable[[str], bool]:
    def _check(output: str) -> bool:
        lowered = output.lower()
        return not any(fb.lower() in lowered for fb in forbidden)
    return _check


def pattern_check(pattern: str) -> Callable[[str], bool]:
    import re
    return lambda output: bool(re.search(pattern, output))
