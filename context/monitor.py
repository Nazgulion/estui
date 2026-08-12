"""Context window monitor — estimates token usage and warns before limits."""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_HISTORY_FILE = Path(__file__).parent.parent / ".context_history.json"


@dataclass
class ContextAssessment:
    tokens_used: int
    tokens_total: int
    usage_pct: float
    remaining: int
    level: str        # green, yellow, orange, red
    action: str       # recommended action
    detail: str = ""

    def to_dict(self) -> dict:
        return {
            "tokens_used": self.tokens_used,
            "tokens_total": self.tokens_total,
            "usage_pct": round(self.usage_pct, 1),
            "remaining": self.remaining,
            "level": self.level,
            "action": self.action,
        }

    def is_critical(self) -> bool:
        return self.level in ("orange", "red")

    def is_warning(self) -> bool:
        return self.level in ("yellow", "orange", "red")


class ContextMonitor:
    def __init__(self, max_tokens: int = 200_000):
        self.max_tokens = max_tokens
        self.turn_count = 0
        self._estimated_tokens = 0
        self._load_history()

    def _load_history(self):
        import json
        if _HISTORY_FILE.exists():
            try:
                data = json.loads(_HISTORY_FILE.read_text())
                self.turn_count = data.get("turn_count", 0)
                self._estimated_tokens = data.get("estimated_tokens", 0)
            except Exception:
                pass

    def _save_history(self):
        import json
        _HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        _HISTORY_FILE.write_text(json.dumps({
            "turn_count": self.turn_count,
            "estimated_tokens": self._estimated_tokens,
        }, indent=2))

    @staticmethod
    def estimate_tokens(text: str) -> int:
        return max(1, len(text) // 4)

    def record_turn(self, text: str):
        self.turn_count += 1
        self._estimated_tokens += self.estimate_tokens(text)
        self._save_history()

    def assess(self, extra_tokens: int = 0) -> ContextAssessment:
        total = self._estimated_tokens + extra_tokens
        usage = total / self.max_tokens

        if usage >= 0.95:
            level = "red"
            action = (
                "CTX RED [95%+]: EMERGENCY. Write checkpoint NOW with all decisions + "
                "remaining work. Compress history using hierarchical summarization. "
                "Save session, then continue in a new session. Do NOT start new analysis."
            )
            detail = "Risk of losing the beginning of the conversation: goals, constraints, decisions."
        elif usage >= 0.85:
            level = "orange"
            action = (
                "CTX ORANGE [85%+]: CRITICAL. Write checkpoint with current state. "
                "Compress early conversation turns into session summary. "
                "Finish current step, then save session and prepare to restore."
            )
            detail = "Model may start losing attention to early content (lost in the middle effect)."
        elif usage >= 0.70:
            level = "yellow"
            action = (
                "CTX YELLOW [70%+]: Write a checkpoint soon. "
                "Summarize completed phases. Continue working but start tracking closely."
            )
            detail = "Approaching the zone where early context begins to fade."
        else:
            level = "green"
            action = "CTX GREEN: Plenty of room. Continue working normally."
            detail = ""

        return ContextAssessment(
            tokens_used=total if total > 0 else self._estimated_tokens,
            tokens_total=self.max_tokens,
            usage_pct=round(usage * 100, 1),
            remaining=self.max_tokens - total,
            level=level,
            action=action,
            detail=detail,
        )

    def simulate_assess(self, tokens: int) -> ContextAssessment:
        return self.assess(extra_tokens=tokens)

    def reset(self):
        self.turn_count = 0
        self._estimated_tokens = 0
        import os
        if _HISTORY_FILE.exists():
            os.remove(_HISTORY_FILE)


def format_assessment(a: ContextAssessment, turns: int = 0) -> str:
    """Format an assessment as a block for injection into the agent's context."""
    bar_width = 20
    filled = int(a.usage_pct / 100 * bar_width)
    bar = "[" + "#" * filled + "-" * (bar_width - filled) + "]"

    turns_info = f"  Turns tracked: {turns}" if turns > 0 else ""
    lines = [
        f"{a.action}",
        "",
        f"  Context usage: {bar} {a.usage_pct:.1f}%  ({a.tokens_used}/{a.tokens_total} tokens, {a.remaining} remaining)",
    ]
    if turns_info.strip():
        lines.append(turns_info)
    if a.detail:
        lines.append(f"  Note: {a.detail}")
    return "\n".join(lines)
