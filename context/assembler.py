from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ContextWindow:
    system_prompt: str = ""
    task_description: str = ""
    conversation_history: list[dict] = field(default_factory=list)
    tool_outputs: list[str] = field(default_factory=list)
    retrieved_memory: list[str] = field(default_factory=list)
    user_query: str = ""

    def assemble(self) -> str:
        parts = []

        if self.system_prompt:
            parts.append(f"[SYSTEM]\n{self.system_prompt}")

        if self.task_description:
            parts.append(f"[TASK]\n{self.task_description}")

        if self.retrieved_memory:
            parts.append(f"[RETRIEVED]\n" + "\n".join(self.retrieved_memory))

        if self.conversation_history:
            history_block = "[HISTORY]\n"
            for turn in self.conversation_history:
                history_block += f"User: {turn.get('user', '')}\n"
                history_block += f"Agent: {turn.get('agent', '')}\n\n"
            parts.append(history_block.strip())

        if self.tool_outputs:
            parts.append(f"[TOOL OUTPUTS]\n" + "\n---\n".join(self.tool_outputs))

        if self.user_query:
            parts.append(f"[QUERY]\n{self.user_query}")

        return "\n\n".join(parts)

    def estimated_tokens(self) -> int:
        return max(1, len(self.assemble()) // 4)

    def remaining_budget(self, max_tokens: int) -> int:
        return max_tokens - self.estimated_tokens()


class ContextAssembler:
    def __init__(self, max_tokens: int = 8000):
        self.max_tokens = max_tokens

    def build(
        self,
        system_prompt: str = "",
        task_description: str = "",
        conversation_history: list[dict] = None,
        tool_outputs: list[str] = None,
        retrieved_memory: list[str] = None,
        user_query: str = "",
    ) -> ContextWindow:
        history = conversation_history or []
        outputs = tool_outputs or []
        memory = retrieved_memory or []

        window = ContextWindow(
            system_prompt=system_prompt,
            task_description=task_description,
            conversation_history=history,
            tool_outputs=outputs,
            retrieved_memory=memory,
            user_query=user_query,
        )

        budget = self.max_tokens
        overhead = self._estimate_overhead(window)
        remaining = budget - overhead

        if remaining <= 0:
            window.conversation_history = self._trim_history(window.conversation_history, budget)
        else:
            window.tool_outputs = self._fit_to_budget(window.tool_outputs, remaining)
            remaining = budget - self._estimate_overhead(window)
            window.conversation_history = self._fit_history_to_budget(window.conversation_history, remaining)

        return window

    def _estimate_overhead(self, window: ContextWindow) -> int:
        base = ContextWindow(
            system_prompt=window.system_prompt,
            task_description=window.task_description,
            user_query=window.user_query,
        )
        return base.estimated_tokens()

    def _trim_history(self, history: list[dict], budget: int) -> list[dict]:
        kept = []
        used = 0
        for turn in reversed(history):
            cost = max(1, (len(turn.get("user", "")) + len(turn.get("agent", ""))) // 4)
            if used + cost > budget * 0.5:
                break
            kept.insert(0, turn)
            used += cost
        return kept

    def _fit_to_budget(self, outputs: list[str], budget: int) -> list[str]:
        kept = []
        used = 0
        for output in reversed(outputs):
            cost = max(1, len(output) // 4)
            if used + cost > budget * 0.3:
                break
            kept.insert(0, output)
            used += cost
        return kept

    def _fit_history_to_budget(self, history: list[dict], budget: int) -> list[dict]:
        kept = []
        used = 0
        for turn in reversed(history):
            cost = max(1, (len(turn.get("user", "")) + len(turn.get("agent", ""))) // 4)
            if used + cost > budget:
                break
            kept.insert(0, turn)
            used += cost
        return kept
