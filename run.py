"""memory-lab CLI — the agent's runtime for opencode sessions."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from datetime import datetime, timedelta

from memory.schemas import Checkpoint, MemoryEntry, MemoryType, SessionSummary, TrustLevel
from memory.store import MemoryStore
from memory.episodic import EpisodicLog
from memory.checkpoint import CheckpointManager, SessionManager, persist_to_semantic_memory
from memory.retrieval import RetrievalEngine
from context.monitor import ContextMonitor, format_assessment, ContextAssessment

STORE_FILE = ROOT / ".memory_store.json"
SESSION_ID = "opencode-session"


class SessionState:
    def __init__(self):
        self.session_id = SESSION_ID
        self.store = MemoryStore()
        self.episodic = EpisodicLog(self.store)
        self.checkpoint_mgr = CheckpointManager()
        self.session_mgr = SessionManager()
        self.retrieval = RetrievalEngine(self.store)
        self._load_store()

    def _load_store(self):
        if not STORE_FILE.exists():
            return
        try:
            data = json.loads(STORE_FILE.read_text())
            for ed in data.get("entries", []):
                ttl = timedelta(seconds=ed["ttl_seconds"]) if ed.get("ttl_seconds") else None
                entry = MemoryEntry(
                    id=ed["id"], content=ed["content"],
                    type=MemoryType(ed["type"]),
                    confidence=ed.get("confidence", 1.0),
                    trust_level=TrustLevel(ed.get("trust_level", "unknown")),
                    created_at=datetime.fromisoformat(ed["created_at"]),
                    session_id=ed.get("session_id", ""),
                    namespace=ed.get("namespace", "default"),
                    metadata=ed.get("metadata", {}),
                    ttl=ttl,
                )
                self.store.write(entry)
        except Exception:
            pass

    def _save_store(self):
        entries = []
        for e in self.store.query():
            entries.append({
                "id": e.id, "content": e.content, "type": e.type.value,
                "confidence": e.confidence, "trust_level": e.trust_level.value,
                "created_at": e.created_at.isoformat(),
                "session_id": e.session_id, "namespace": e.namespace,
                "metadata": e.metadata,
                "ttl_seconds": int(e.ttl.total_seconds()) if e.ttl else None,
            })
        ROOT.mkdir(parents=True, exist_ok=True)
        STORE_FILE.write_text(json.dumps(
            {"entries": entries, "last_saved": datetime.now().isoformat()},
            indent=2,
        ))

    def set_goal(self, goal: str):
        self.store.write_working(goal, "current_goal", ttl_seconds=86400)
        self._save_store()

    def checkpoint(self, note: str = ""):
        goal = self.store.read_working("current_goal") or ""
        cp = Checkpoint(task_goal=goal)
        if note:
            cp.remaining.append(note)
        self.checkpoint_mgr.write_checkpoint(cp)
        self._save_store()
        return cp.id

    def decide(self, decision: str, reasoning: str):
        self.episodic.record_decision(
            decision=decision, reasoning=reasoning, alternatives=[], session_id=self.session_id)
        self._save_store()

    def did(self, action: str, tool: str, input_summary: str, output_summary: str):
        self.episodic.record_action(
            action=action, tool=tool, input_summary=input_summary,
            output_summary=output_summary, success=True, session_id=self.session_id)
        self._save_store()

    def failed(self, attempt: str, error: str, root_cause: str):
        self.episodic.record_failure(
            attempt=attempt, error=error, root_cause=root_cause, session_id=self.session_id)
        self._save_store()

    def remember(self, content: str, confidence: float = 0.85):
        persist_to_semantic_memory(self.store, content, confidence=confidence)
        self._save_store()

    def recall(self, query: str, budget: int = 3000) -> dict:
        result = self.retrieval.retrieve(query=query, token_budget=budget)
        return {
            "source": result.source,
            "entries": [
                {"type": e.type.value, "content": e.content[:300],
                 "confidence": round(e.confidence, 2)}
                for e in result.entries
            ],
            "tokens": f"{result.budget_used}/{result.budget_total}",
        }

    def save_session(self):
        goal = self.store.read_working("current_goal") or "Unknown"
        actions = self.episodic.get_recent_actions(limit=20, session_id=self.session_id)
        decisions = self.episodic.get_decisions(session_id=self.session_id)
        summary = SessionSummary(
            session_id=self.session_id, goal=goal,
            key_decisions=[d.metadata.get("decision", "") for d in decisions[:5] if d.metadata.get("decision")],
            state=f"{self.store.size} entries. Actions: {len(actions)}. Decisions: {len(decisions)}.",
            next_steps=["Restore to continue."],
        )
        self.session_mgr.write_summary(summary)
        self._save_store()
        return summary.id

    def restore_context(self) -> str | None:
        return self.session_mgr.restore_context_prompt()

    def status(self) -> dict:
        self.store.run_maintenance()
        type_counts = {}
        for e in self.store.query():
            t = e.type.value
            type_counts[t] = type_counts.get(t, 0) + 1
        latest = self.checkpoint_mgr.load_latest_checkpoint()
        monitor = ContextMonitor()
        ctx_assess = monitor.assess()
        return {
            "entries": self.store.size,
            "types": type_counts,
            "goal": self.store.read_working("current_goal"),
            "checkpoint": latest.task_goal if latest else None,
            "has_prior_session": self.session_mgr.has_prior_session(),
            "context": {
                "level": ctx_assess.level,
                "usage_pct": ctx_assess.usage_pct,
                "turns_tracked": monitor.turn_count,
            },
        }

    def assess_context(self, estimated_tokens: int = 0, max_tokens: int = 200_000) -> ContextAssessment:
        monitor = ContextMonitor(max_tokens=max_tokens)
        return monitor.simulate_assess(estimated_tokens)

    def record_turn(self, text: str, max_tokens: int = 200_000):
        monitor = ContextMonitor(max_tokens=max_tokens)
        monitor.record_turn(text)
        return monitor.assess()


def main():
    if len(sys.argv) < 2:
        print("""memory-lab agent runtime

  GOAL & CHECKPOINTS
    goal      <text>              Set the current task goal
    cp        [note]              Write a checkpoint

  EPISODIC LOG (what happened)
    decide    <decision> <reason>  Log a decision and its reasoning
    did       <action> <tool> <input-summary> <output-summary>
                                   Log a completed action
    failed    <attempt> <error> <cause>
                                   Log a failure with root cause

  DURABLE MEMORY
    remember  <content>            Store in semantic memory (confidence 0.85)
    recall    <query>              Search all memory (working -> episodic -> semantic)

  SESSION LIFECYCLE
    save                           Save session for later restore
    restore                        Show session resume prompt

  CONTEXT MONITOR
    usage     <tokens> [max]       Check context usage levels
    turn      <text>               Track a conversation turn

  STATUS
    status                         Full system state""")
        return

    cmd = sys.argv[1]
    args = sys.argv[2:]
    s = SessionState()

    def require(n, label):
        if len(args) < n:
            print(f"{label}: need {n} args, got {len(args)}")
            sys.exit(1)

    if cmd == "goal":
        s.set_goal(" ".join(args))
        print("ok")

    elif cmd == "cp":
        s.checkpoint(" ".join(args) if args else "")
        print("ok")

    elif cmd == "decide":
        require(2, "decide <decision> <reasoning>")
        s.decide(args[0], " ".join(args[1:]))
        print("ok")

    elif cmd == "did":
        require(4, "did <action> <tool> <input> <output>")
        s.did(args[0], args[1], args[2], " ".join(args[3:]))
        print("ok")

    elif cmd == "failed":
        require(3, "failed <attempt> <error> <root_cause>")
        s.failed(args[0], args[1], " ".join(args[2:]))
        print("ok")

    elif cmd == "remember":
        require(1, "remember <content>")
        s.remember(" ".join(args))
        print("ok")

    elif cmd == "recall":
        print(json.dumps(s.recall(" ".join(args)), indent=2))

    elif cmd == "save":
        sid = s.save_session()
        print(f"saved {sid}")

    elif cmd == "restore":
        ctx = s.restore_context()
        print(ctx if ctx else "(no prior session)")

    elif cmd == "status":
        print(json.dumps(s.status(), indent=2))

    elif cmd == "usage":
        estimated = int(args[0]) if args else 0
        max_tok = int(args[1]) if len(args) > 1 else 200_000
        a = s.assess_context(estimated, max_tok)
        print(format_assessment(a))

    elif cmd == "turn":
        text = " ".join(args)
        max_tok = 200_000
        a = s.record_turn(text, max_tok)
        print(format_assessment(a))

    else:
        print(f"Unknown: {cmd}")


if __name__ == "__main__":
    main()
