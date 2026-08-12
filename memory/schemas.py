from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Optional
import uuid


class MemoryType(Enum):
    WORKING = "working"
    EPISODIC = "episodic"
    SEMANTIC = "semantic"
    PROCEDURAL = "procedural"


class TrustLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


@dataclass
class MemoryEntry:
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    content: str = ""
    type: MemoryType = MemoryType.WORKING
    confidence: float = 1.0
    trust_level: TrustLevel = TrustLevel.UNKNOWN
    created_at: datetime = field(default_factory=datetime.now)
    ttl: Optional[timedelta] = None
    session_id: str = ""
    namespace: str = "default"
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def age(self) -> timedelta:
        return datetime.now() - self.created_at

    @property
    def expired(self) -> bool:
        if self.ttl is None:
            return False
        return self.age > self.ttl

    @property
    def reliability(self) -> float:
        trust_multipliers = {
            TrustLevel.HIGH: 1.0,
            TrustLevel.MEDIUM: 0.7,
            TrustLevel.LOW: 0.4,
            TrustLevel.UNKNOWN: 0.5,
        }
        return self.confidence * trust_multipliers[self.trust_level]


@dataclass
class Checkpoint:
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    task_goal: str = ""
    completed: list[str] = field(default_factory=list)
    decisions: list[dict[str, str]] = field(default_factory=list)
    remaining: list[str] = field(default_factory=list)
    resume_requirements: list[str] = field(default_factory=list)
    failed_attempts: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class SessionSummary:
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    session_id: str = ""
    goal: str = ""
    key_decisions: list[str] = field(default_factory=list)
    state: str = ""
    next_steps: list[str] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_restore_prompt(self) -> str:
        lines = [
            f"Session goal: {self.goal}",
            "",
            "Key decisions:",
        ]
        for d in self.key_decisions:
            lines.append(f"  - {d}")
        lines.append("")
        lines.append(f"Current state: {self.state}")
        lines.append("")
        lines.append("Next steps:")
        for s in self.next_steps:
            lines.append(f"  - {s}")
        if self.constraints:
            lines.append("")
            lines.append("Constraints:")
            for c in self.constraints:
                lines.append(f"  - {c}")
        return "\n".join(lines)
