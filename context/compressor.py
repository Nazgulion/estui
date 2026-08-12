import re


def compress_tool_output(output: str, max_chars: int = 500) -> str:
    if len(output) <= max_chars:
        return output
    return output[:max_chars] + f"\n... [truncated {len(output) - max_chars} chars]"


def summarize_tool_output(output: str) -> str:
    lines = output.strip().split("\n")
    key_lines = []
    for line in lines[:20]:
        if line.strip() and not line.strip().startswith("#"):
            key_lines.append(line.strip())
    summary = f"[{len(lines)} lines, {len(output)} chars] Key content:\n"
    summary += "\n".join(key_lines[:5])
    if len(key_lines) > 5:
        summary += f"\n... (+{len(key_lines) - 5} more significant lines)"
    return summary


def summarize_conversation_history(history: list[dict], max_turns: int = 5) -> list[dict]:
    if len(history) <= max_turns:
        return history
    early = history[:2]
    recent = history[-(max_turns - 2):]
    summary_turn = {
        "user": f"[{len(history) - max_turns} turns omitted]",
        "agent": "[Conversation compressed for context efficiency]",
    }
    return early + [summary_turn] + recent


def rolling_window_summary(history: list[dict], window_size: int = 10) -> str:
    if len(history) <= window_size:
        return "\n".join(
            f"User: {t.get('user', '')}\nAgent: {t.get('agent', '')}" for t in history
        )

    recent = history[-window_size:]
    summary_lines = [f"[Rolling window: {window_size}/{len(history)} turns shown]\n"]
    for t in recent:
        summary_lines.append(f"User: {t.get('user', '')}\nAgent: {t.get('agent', '')}")
    return "\n".join(summary_lines)


def extract_key_facts(text: str) -> list[str]:
    sentences = re.split(r'(?<=[.!?])\s+', text)
    facts = []
    for s in sentences:
        s = s.strip()
        if len(s) > 20 and not s.startswith("Note") and not s.startswith("Let me"):
            facts.append(s)
    return facts[:10]


def hierarchical_summarize(text: str, levels: int = 3) -> list[str]:
    summaries = [text]
    current = text
    for _ in range(levels - 1):
        facts = extract_key_facts(current)
        if not facts:
            break
        current = " | ".join(facts)
        summaries.append(current)
    return summaries
