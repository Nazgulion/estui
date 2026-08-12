from dataclasses import dataclass, field
from typing import Optional

from .schemas import MemoryEntry, MemoryType
from .store import MemoryStore


@dataclass
class RetrievalResult:
    entries: list[MemoryEntry] = field(default_factory=list)
    budget_used: int = 0
    budget_total: int = 0
    source: str = ""

    @property
    def remaining_budget(self) -> int:
        return self.budget_total - self.budget_used

    @property
    def within_budget(self) -> bool:
        return self.budget_used <= self.budget_total


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


class RetrievalEngine:
    def __init__(self, store: MemoryStore, default_budget: int = 2000):
        self.store = store
        self.default_budget = default_budget

    def retrieve(
        self,
        query: str,
        namespace: str = "default",
        token_budget: Optional[int] = None,
        min_confidence: float = 0.3,
        include_episodic: bool = True,
        include_semantic: bool = True,
        include_procedural: bool = True,
    ) -> RetrievalResult:
        budget = token_budget or self.default_budget
        result = RetrievalResult(budget_total=budget)

        working = self._retrieve_working(namespace)
        result.entries, result.budget_used = self._fit_budget(working, budget)
        result.source = "working_memory"

        if result.remaining_budget > 0 and include_episodic:
            episodic = self._retrieve_episodic(query, namespace, min_confidence)
            remaining = [e for e in episodic if e.id not in {x.id for x in result.entries}]
            added, tokens = self._fit_budget(remaining, result.remaining_budget)
            result.entries.extend(added)
            result.budget_used += tokens
            if added:
                result.source += "+episodic"

        if result.remaining_budget > 0 and include_semantic:
            semantic = self._retrieve_semantic(query, namespace, min_confidence)
            remaining = [e for e in semantic if e.id not in {x.id for x in result.entries}]
            added, tokens = self._fit_budget(remaining, result.remaining_budget)
            result.entries.extend(added)
            result.budget_used += tokens
            if added:
                result.source += "+semantic"

        if result.remaining_budget > 0 and include_procedural:
            procedural = self._retrieve_procedural(query, namespace)
            remaining = [e for e in procedural if e.id not in {x.id for x in result.entries}]
            added, tokens = self._fit_budget(remaining, result.remaining_budget)
            result.entries.extend(added)
            result.budget_used += tokens
            if added:
                result.source += "+procedural"

        return result

    def _retrieve_working(self, namespace: str) -> list[MemoryEntry]:
        return self.store.query(memory_type=MemoryType.WORKING, namespace=namespace)

    def _retrieve_episodic(self, query: str, namespace: str, min_confidence: float) -> list[MemoryEntry]:
        entries = self.store.query(
            memory_type=MemoryType.EPISODIC,
            namespace=namespace,
            min_confidence=min_confidence,
        )
        if query:
            entries = self._simple_rank(entries, query)
        return entries

    def _retrieve_semantic(self, query: str, namespace: str, min_confidence: float) -> list[MemoryEntry]:
        entries = self.store.query(
            memory_type=MemoryType.SEMANTIC,
            namespace=namespace,
            min_confidence=min_confidence,
        )
        if query:
            entries = self._simple_rank(entries, query)
        return entries

    def _retrieve_procedural(self, query: str, namespace: str) -> list[MemoryEntry]:
        entries = self.store.query(memory_type=MemoryType.PROCEDURAL, namespace=namespace)
        if query:
            entries = self._simple_rank(entries, query)
        return entries

    def _simple_rank(self, entries: list[MemoryEntry], query: str) -> list[MemoryEntry]:
        query_lower = query.lower()
        scored = []
        for entry in entries:
            content_lower = entry.content.lower()
            score = 0
            if query_lower in content_lower:
                score += 10
            query_terms = query_lower.split()
            for term in query_terms:
                if term in content_lower:
                    score += 2
            score += entry.confidence * 5
            score += entry.reliability * 5
            scored.append((score, entry))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [e for _, e in scored]

    def _fit_budget(self, entries: list[MemoryEntry], budget: int) -> tuple[list[MemoryEntry], int]:
        selected = []
        used = 0
        for entry in entries:
            cost = estimate_tokens(entry.content)
            if used + cost <= budget:
                selected.append(entry)
                used += cost
        return selected, used

    def inject_into_context(
        self,
        retrieval: RetrievalResult,
        context: str,
        placement: str = "end",
    ) -> str:
        if not retrieval.entries:
            return context

        memory_block = "\n\n[Retrieved from memory]\n"
        for entry in retrieval.entries:
            memory_block += f"- [{entry.type.value}] {entry.content[:300]}\n"

        if placement == "end":
            return context + memory_block
        elif placement == "start":
            return memory_block + "\n" + context
        else:
            midpoint = len(context) // 2
            return context[:midpoint] + memory_block + context[midpoint:]
