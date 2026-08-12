from datetime import datetime, timedelta
from threading import Lock
from typing import Optional

from .schemas import MemoryEntry, MemoryType, TrustLevel


class MemoryStore:
    def __init__(self):
        self._store: dict[str, MemoryEntry] = {}
        self._lock = Lock()
        self._index: dict[MemoryType, set[str]] = {t: set() for t in MemoryType}
        self._namespace_index: dict[str, set[str]] = {}

    def write(self, entry: MemoryEntry) -> str:
        with self._lock:
            self._store[entry.id] = entry
            self._index[entry.type].add(entry.id)
            self._namespace_index.setdefault(entry.namespace, set()).add(entry.id)
            return entry.id

    def read(self, entry_id: str) -> Optional[MemoryEntry]:
        with self._lock:
            entry = self._store.get(entry_id)
            if entry and entry.expired:
                self._delete(entry_id)
                return None
            return entry

    def update(self, entry_id: str, **kwargs) -> bool:
        with self._lock:
            entry = self._store.get(entry_id)
            if not entry or entry.expired:
                return False
            for key, value in kwargs.items():
                if hasattr(entry, key):
                    setattr(entry, key, value)
            return True

    def delete(self, entry_id: str) -> bool:
        with self._lock:
            return self._delete(entry_id)

    def _delete(self, entry_id: str) -> bool:
        entry = self._store.pop(entry_id, None)
        if entry is None:
            return False
        self._index[entry.type].discard(entry_id)
        idx = self._namespace_index.get(entry.namespace)
        if idx:
            idx.discard(entry_id)
        return True

    def query(
        self,
        memory_type: Optional[MemoryType] = None,
        namespace: Optional[str] = None,
        min_confidence: float = 0.0,
        min_reliability: float = 0.0,
    ) -> list[MemoryEntry]:
        with self._lock:
            self._purge_expired()
            entries = list(self._store.values())

            if memory_type is not None:
                entries = [e for e in entries if e.type == memory_type]
            if namespace is not None:
                entries = [e for e in entries if e.namespace == namespace]
            if min_confidence > 0:
                entries = [e for e in entries if e.confidence >= min_confidence]
            if min_reliability > 0:
                entries = [e for e in entries if e.reliability >= min_reliability]

            return sorted(entries, key=lambda e: e.created_at, reverse=True)

    def run_maintenance(self) -> dict:
        with self._lock:
            removed = self._purge_expired()
            deduped = self._deduplicate()
            decayed = self._apply_confidence_decay()
            return {"expired_removed": removed, "duplicates_merged": deduped, "confidence_decayed": decayed}

    def _purge_expired(self) -> int:
        expired_ids = [eid for eid, e in self._store.items() if e.expired]
        for eid in expired_ids:
            self._delete(eid)
        return len(expired_ids)

    def _deduplicate(self) -> int:
        seen: dict[str, str] = {}
        duplicates = 0
        for eid, entry in list(self._store.items()):
            key = (entry.content.strip().lower(), entry.type.value, entry.namespace)
            if key in seen:
                existing = self._store.get(seen[key])
                if existing and existing.created_at < entry.created_at:
                    self._delete(seen[key])
                    seen[key] = eid
                else:
                    self._delete(eid)
                    duplicates += 1
            else:
                seen[key] = eid
        return duplicates

    def _apply_confidence_decay(self, rate: float = 0.01) -> int:
        decayed = 0
        now = datetime.now()
        for entry in list(self._store.values()):
            if entry.type in (MemoryType.WORKING, MemoryType.EPISODIC):
                hours_old = (now - entry.created_at).total_seconds() / 3600
                if hours_old > 1:
                    new_confidence = max(0.0, entry.confidence - rate * hours_old)
                    if new_confidence != entry.confidence:
                        entry.confidence = new_confidence
                        decayed += 1
        return decayed

    def write_working(self, content: str, key: str, namespace: str = "default", ttl_seconds: int = 3600) -> str:
        entry = MemoryEntry(
            content=content,
            type=MemoryType.WORKING,
            namespace=namespace,
            trust_level=TrustLevel.HIGH,
            metadata={"key": key},
            ttl=timedelta(seconds=ttl_seconds),
        )
        return self.write(entry)

    def read_working(self, key: str, namespace: str = "default") -> Optional[str]:
        entries = self.query(memory_type=MemoryType.WORKING, namespace=namespace)
        for e in entries:
            if e.metadata.get("key") == key:
                return e.content
        return None

    @property
    def size(self) -> int:
        return len(self._store)
