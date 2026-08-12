from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional

from .self_check import CheckResult, SelfChecker


@dataclass
class LoopResult:
    final_output: str = ""
    iterations: int = 0
    passed: bool = False
    outputs_kept: list[str] = field(default_factory=list)
    outputs_discarded: list[str] = field(default_factory=list)
    scores: list[float] = field(default_factory=list)
    stopped_by_limit: bool = False
    started_at: datetime = field(default_factory=datetime.now)
    ended_at: Optional[datetime] = None

    @property
    def keep_rate(self) -> float:
        if self.iterations == 0:
            return 0.0
        return len(self.outputs_kept) / self.iterations

    @property
    def total_duration_seconds(self) -> float:
        if self.ended_at is None:
            return (datetime.now() - self.started_at).total_seconds()
        return (self.ended_at - self.started_at).total_seconds()


class LoopRunner:
    def __init__(
        self,
        generate: Callable[[str, str], str],
        checker: SelfChecker,
        max_iterations: int = 8,
        keep_best: bool = True,
    ):
        self.generate = generate
        self.checker = checker
        self.max_iterations = max_iterations
        self.keep_best = keep_best

    def run(self, task: str, initial_context: str = "") -> LoopResult:
        result = LoopResult()
        best_output = ""
        best_score = -1.0

        for i in range(self.max_iterations):
            context = self._build_context(task, initial_context, result, i)
            output = self.generate(task, context)
            check = self.checker.check(output)

            result.iterations = i + 1
            result.scores.append(check.overall_score)

            if check.passed:
                result.outputs_kept.append(output)
                result.final_output = output
                result.passed = True
                result.ended_at = datetime.now()
                return result

            if self.keep_best and check.overall_score > best_score:
                best_score = check.overall_score
                best_output = output

            if check.overall_score >= 0.5:
                result.outputs_kept.append(output)
            else:
                result.outputs_discarded.append(output)

        result.stopped_by_limit = True
        result.ended_at = datetime.now()

        if best_output and self.keep_best:
            result.final_output = best_output
        elif result.outputs_kept:
            result.final_output = result.outputs_kept[-1]
        else:
            result.final_output = f"[Loop exhausted after {self.max_iterations} iterations. No output passed.]"

        return result

    def _build_context(self, task: str, initial: str, result: LoopResult, iteration: int) -> str:
        parts = [initial]
        if result.outputs_kept:
            parts.append(f"\nPrevious best output score: {max(result.scores):.1%}")
        if result.outputs_discarded:
            parts.append(f"Discarded outputs: {len(result.outputs_discarded)}")
        parts.append(f"Iteration {iteration + 1}/{self.max_iterations}")
        return "\n".join(parts)


def make_generator(agent_fn: Callable[[str], str]) -> Callable[[str, str], str]:
    return lambda task, context: agent_fn(f"{context}\n\nTask: {task}")
