import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from .schemas import Checkpoint, MemoryEntry, MemoryType, SessionSummary, TrustLevel
from .store import MemoryStore


CHECKPOINT_DIR = Path(__file__).parent.parent / "checkpoints"
SESSION_DIR = Path(__file__).parent.parent / "sessions"


def _ensure_dirs():
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_DIR.mkdir(parents=True, exist_ok=True)


class CheckpointManager:
    def write_checkpoint(self, checkpoint: Checkpoint) -> Path:
        _ensure_dirs()
        path = CHECKPOINT_DIR / f"checkpoint_{checkpoint.id}.json"
        data = {
            "id": checkpoint.id,
            "task_goal": checkpoint.task_goal,
            "completed": checkpoint.completed,
            "decisions": checkpoint.decisions,
            "remaining": checkpoint.remaining,
            "resume_requirements": checkpoint.resume_requirements,
            "failed_attempts": checkpoint.failed_attempts,
            "timestamp": checkpoint.timestamp.isoformat(),
        }
        path.write_text(json.dumps(data, indent=2))
        return path

    def load_latest_checkpoint(self) -> Optional[Checkpoint]:
        _ensure_dirs()
        files = sorted(CHECKPOINT_DIR.glob("checkpoint_*.json"), key=os.path.getmtime, reverse=True)
        if not files:
            return None
        return self._load_from_path(files[0])

    def load_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        path = CHECKPOINT_DIR / f"checkpoint_{checkpoint_id}.json"
        if not path.exists():
            return None
        return self._load_from_path(path)

    def _load_from_path(self, path: Path) -> Checkpoint:
        data = json.loads(path.read_text())
        return Checkpoint(
            id=data["id"],
            task_goal=data["task_goal"],
            completed=data["completed"],
            decisions=data["decisions"],
            remaining=data["remaining"],
            resume_requirements=data["resume_requirements"],
            failed_attempts=data.get("failed_attempts", []),
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )

    def to_restore_prompt(self, checkpoint: Checkpoint) -> str:
        lines = [f"Goal: {checkpoint.task_goal}", "", "Completed so far:"]
        for item in checkpoint.completed:
            lines.append(f"  - {item}")
        lines.append("")
        lines.append("Decisions made and why:")
        for d in checkpoint.decisions:
            lines.append(f"  - {d.get('decision', '')}: {d.get('reason', '')}")
        lines.append("")
        lines.append("Remaining:")
        for item in checkpoint.remaining:
            lines.append(f"  - {item}")
        if checkpoint.failed_attempts:
            lines.append("")
            lines.append("What didn't work:")
            for item in checkpoint.failed_attempts:
                lines.append(f"  - {item}")
        lines.append("")
        lines.append("To resume in a new session:")
        for item in checkpoint.resume_requirements:
            lines.append(f"  - {item}")
        return "\n".join(lines)


class SessionManager:
    def write_summary(self, summary: SessionSummary) -> Path:
        _ensure_dirs()
        path = SESSION_DIR / f"session_{summary.id}.json"
        data = {
            "id": summary.id,
            "session_id": summary.session_id,
            "goal": summary.goal,
            "key_decisions": summary.key_decisions,
            "state": summary.state,
            "next_steps": summary.next_steps,
            "constraints": summary.constraints,
            "timestamp": summary.timestamp.isoformat(),
        }
        path.write_text(json.dumps(data, indent=2))
        return path

    def load_latest_summary(self) -> Optional[SessionSummary]:
        _ensure_dirs()
        files = sorted(SESSION_DIR.glob("session_*.json"), key=os.path.getmtime, reverse=True)
        if not files:
            return None
        return self._load_from_path(files[0])

    def _load_from_path(self, path: Path) -> SessionSummary:
        data = json.loads(path.read_text())
        return SessionSummary(
            id=data["id"],
            session_id=data.get("session_id", ""),
            goal=data["goal"],
            key_decisions=data["key_decisions"],
            state=data["state"],
            next_steps=data["next_steps"],
            constraints=data.get("constraints", []),
            timestamp=datetime.fromisoformat(data["timestamp"]),
        )

    def has_prior_session(self) -> bool:
        _ensure_dirs()
        return any(SESSION_DIR.glob("session_*.json"))

    def restore_context_prompt(self) -> Optional[str]:
        summary = self.load_latest_summary()
        if summary is None:
            return None
        return summary.to_restore_prompt()


def persist_to_semantic_memory(store: MemoryStore, content: str, namespace: str = "default", confidence: float = 0.8) -> str:
    entry = MemoryEntry(
        content=content,
        type=MemoryType.SEMANTIC,
        namespace=namespace,
        confidence=confidence,
        trust_level=TrustLevel.MEDIUM,
    )
    return store.write(entry)
