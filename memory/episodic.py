from datetime import datetime
from typing import Optional

from .schemas import MemoryEntry, MemoryType, TrustLevel
from .store import MemoryStore


class EpisodicLog:
    def __init__(self, store: MemoryStore):
        self.store = store

    def record_action(
        self,
        action: str,
        tool: str,
        input_summary: str,
        output_summary: str,
        success: bool,
        session_id: str = "",
        namespace: str = "default",
    ) -> str:
        content = (
            f"Action: {action}\n"
            f"Tool: {tool}\n"
            f"Input: {input_summary}\n"
            f"Output: {output_summary}\n"
            f"Success: {success}"
        )
        entry = MemoryEntry(
            content=content,
            type=MemoryType.EPISODIC,
            session_id=session_id,
            namespace=namespace,
            trust_level=TrustLevel.HIGH,
            metadata={
                "action": action,
                "tool": tool,
                "success": success,
            },
        )
        return self.store.write(entry)

    def record_decision(
        self,
        decision: str,
        reasoning: str,
        alternatives: list[str],
        session_id: str = "",
        namespace: str = "default",
    ) -> str:
        content = (
            f"Decision: {decision}\n"
            f"Reasoning: {reasoning}\n"
            f"Alternatives considered: {', '.join(alternatives)}"
        )
        entry = MemoryEntry(
            content=content,
            type=MemoryType.EPISODIC,
            session_id=session_id,
            namespace=namespace,
            trust_level=TrustLevel.HIGH,
            metadata={
                "decision": decision,
                "alternatives": alternatives,
            },
        )
        return self.store.write(entry)

    def record_failure(
        self,
        attempt: str,
        error: str,
        root_cause: str,
        session_id: str = "",
        namespace: str = "default",
    ) -> str:
        content = (
            f"Failed attempt: {attempt}\n"
            f"Error: {error}\n"
            f"Root cause: {root_cause}"
        )
        entry = MemoryEntry(
            content=content,
            type=MemoryType.EPISODIC,
            session_id=session_id,
            namespace=namespace,
            trust_level=TrustLevel.MEDIUM,
            metadata={
                "attempt": attempt,
                "error": error,
                "success": False,
            },
        )
        return self.store.write(entry)

    def get_recent_actions(
        self, limit: int = 10, session_id: Optional[str] = None, namespace: str = "default"
    ) -> list[MemoryEntry]:
        entries = self.store.query(memory_type=MemoryType.EPISODIC, namespace=namespace)
        if session_id:
            entries = [e for e in entries if e.session_id == session_id]
        return entries[:limit]

    def get_decisions(self, session_id: Optional[str] = None, namespace: str = "default") -> list[MemoryEntry]:
        entries = self.store.query(memory_type=MemoryType.EPISODIC, namespace=namespace)
        if session_id:
            entries = [e for e in entries if e.session_id == session_id]
        return [e for e in entries if e.metadata.get("decision")]
