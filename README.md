# memory-lab template

Agent runtime for opencode sessions. Implements the **Agents, Loops, Graphs** and **Context vs Memory** patterns from Anthropic's agent engineering guides.

Drop this into any project folder and your LLM agent (opencode) gains persistent memory, session lifecycle, and context monitoring.

## Quick start

```bash
# Copy into your project
cp -r template/* my-project/

cd my-project
python3 run.py status    # verify clean start
```

## Usage during a session

The agent (opencode) invokes `python3 run.py` as needed:

```bash
# Set goal + checkpoint
python3 run.py goal "Build authentication system"
python3 run.py cp "Starting auth module"

# Log decisions and actions as work progresses
python3 run.py decide "Use JWT tokens" "Stateless, standard, well-supported"
python3 run.py did "auth_design" "code_review" "How to structure auth?" "Use middleware pattern with JWT verification"

# Log failures with root cause
python3 run.py failed "attempted simple token" "Token didn't refresh properly" "Missing refresh token rotation logic"

# Store durable facts
python3 run.py remember "Auth middleware sits before route handlers, extracts JWT from Authorization header"

# Search memory (tiered: working -> episodic -> semantic)
python3 run.py recall "authentication JWT"

# Monitor context window usage
python3 run.py usage 140000     # check at ~70%
python3 run.py usage 180000     # check at ~90% — ORANGE warning triggers checkpoint

# Save session for restore later
python3 run.py save

# On new session, restore context
python3 run.py restore
```

## Context monitoring levels

| Level   | Threshold | Action |
|---------|-----------|--------|
| GREEN   | 0-70%     | Work normally |
| YELLOW  | 70-85%    | Write checkpoint, summarize completed work |
| ORANGE  | 85-95%    | Checkpoint now, compress history, finish current step, save |
| RED     | 95%+      | Emergency: checkpoint everything, compress, save, continue in new session |

## Memory types

| Type     | Purpose | Lifespan |
|----------|---------|----------|
| working  | Current task state (goal, session ID, output format) | TTL-based (default 2h) |
| episodic | Actions, decisions, failures — what happened | Confidence decay over time |
| semantic | Durable facts and domain knowledge | Permanent (until maintenance) |

## Files generated (auto-created)

```
my-project/
├── .memory_store.json      # All memory entries
├── .context_history.json   # Context window tracking
├── checkpoints/            # Checkpoint JSON files
└── sessions/               # Session summary JSON files
```

## Architecture

```
run.py           CLI entry point (the agent's console)
memory/          Memory system
  schemas.py     MemoryEntry, Checkpoint, SessionSummary types
  store.py       Thread-safe in-memory KV store with TTL + confidence
  episodic.py    Action/decision/failure logging
  checkpoint.py  File-based checkpoint + session persistence
  retrieval.py   Tiered retrieval (working -> episodic -> semantic + budget)
loop/            Loop system
  self_check.py  Hard criteria that can fail, rubric-based verification
  runner.py      Iterate -> check -> keep/discard -> stop
graph/           Graph system
  node.py        Nodes, edges, fake edge test, dependency detection
  diamond.py     Parallel fan-out -> converge pattern
  checker.py     Triple-check verification with fresh context
context/         Context engineering
  assembler.py   Strategic placement, token budgeting, history trimming
  compressor.py  Tool output truncation, rolling window, summarization
  monitor.py     Context usage estimation + threshold warnings
```

## Requirements

Python 3.10+. No external dependencies for core modules.
