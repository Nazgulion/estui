# estui

Local web dashboard for copying Elasticsearch indices between two clusters.

## Run

Configure `sources`, `targets`, and optionally `port` in `config.yaml`, then run:

```bash
go run .
# Override the configured port:
go run . -port 8080
# Or use another configuration file:
go run . -port 8080 /path/to/config.yaml
```

The configured port in this checkout is 8082; the default without a port setting
is 8080. Open `http://localhost:8082`. The server listens only on `127.0.0.1`,
accepts localhost Host headers for its configured port, and rejects cross-origin
requests. For remote administration, use an SSH tunnel with the same local port,
for example `ssh -L 8082:127.0.0.1:8082 user@server`. Local processes are trusted;
this dashboard is not an authenticated multi-user service.

## Migration behavior

- Both clusters must return distinct, valid cluster UUIDs before migration starts.
  Resume verifies that the connected clusters match the saved source and target.
- Connections cannot change during a migration.
- Native reindex is attempted first unless proxy mode is selected. Scroll/bulk
  fallback retains document IDs and custom routing, validates every response,
  and releases the latest scroll cursor on completion or failure.
- Fallback after native reindex is allowed only after a definite rejection or a
  confirmed terminal task failure. If starting or polling a task has an unknown
  outcome, the migration stops instead of launching another writer. A saved task
  ID must be confirmed stopped before Continue can restart the index. When no ID
  was returned, inspect Elasticsearch tasks before abandoning and starting again.
- The overwrite choice is saved with the migration. Completed and skipped indices
  are not retried. Older checkpoints without an overwrite choice do not authorize
  deleting existing indices. If an unfinished index already exists and overwrite
  was disabled, resume stops for that index rather than silently treating a
  partial copy as skipped; inspect it and start a new migration with the desired
  overwrite choice.
- Every job outcome, including failures and skips, is saved atomically in
  `migration_state.json`. Successful batches appear in history. Destination
  indices are closed after successful migration, as in the existing workflow.

For custom routing semantics, see Elastic's
[routing documentation](https://www.elastic.co/docs/reference/elasticsearch/mapping-reference/mapping-routing-field).

## Development checks

```bash
go test ./...
go test -race ./...
go vet ./...
```

Tests use mock transports and temporary localhost HTTP servers. They do not
connect to configured clusters or modify the checkout's migration state.

## Agent memory tools


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
