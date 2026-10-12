# estui

Local web dashboard for copying and reindexing Elasticsearch indices, within or between clusters.

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

## Browsing large index lists

The Indices tab and both migration panels show 25 rows per page by default.
Choose 10, 25, 50, or 100 rows, use the first/previous/next/last buttons, or enter a
page number. Each panel keeps its own page and page size.

Regex search always filters the complete cluster list before pagination. Changing
the filter returns to the first matching page. Selections survive page changes;
**All matches** and **Clear matches** apply to every filtered result across all
pages. Refreshes and index actions preserve the filter and page, adjusting the
page only when the remaining results no longer fill it.

## Reindex with a different mapping

1. Select indices in the Migration tab and choose the direction. For a local
   reindex, connect both sides to the same cluster.
2. Click **Reindex with mapping**. For each index, paste mapping JSON or upload a
   JSON file (up to 1 MiB). Accepted forms are the mappings object itself,
   `{"mappings": {...}}`, or that index's `GET /index/_mapping` response.
3. Edit the suggested `tmp_reindexed_<name>_<suffix>` destination if needed.
   Choose **Keep original** (default), **Back up original, then remove old name**,
   or **Remove original without a backup**. The backup name defaults to
   `original_<name>_<suffix>` and is editable.
4. Click **Review steps** to inspect the source/destination, names, supplied
   mapping, document filter, verification steps, and original-retention policy.
   Source removal requires a separate acknowledgement. Then click **Start reindex**.

The new index is created with the supplied mapping before documents are copied.
The original mapping is never modified. All destination and backup names must be
unused; this flow cannot overwrite existing indices. Mappings, source/destination
UUIDs, retention choices, and progress are saved for Continue.

Backups are created **on the source cluster** with the original mapping and **all
original documents**, including documents excluded by a migration filter. They
are independently verified and closed with writes blocked before the original
name is removed. A filtered migration cannot remove the original without a full
backup. Backup copies are not snapshots: aliases and lifecycle policies are
withheld, and pipelines are disabled. Removing an original also removes its
aliases; application cutover is a separate operator action.

Only after destination verification (and backup verification when selected) may
estui delete the original. An uncertain delete cannot be repeated by Continue or
forgotten with Abandon; it requires manual reconciliation. After an acknowledged
removal, Continue verifies the destination and backup against their saved
fingerprints and mapping metadata without requiring the removed source. Partial
copies are retained after failure; a retry may allocate a fresh name, displayed
in the job log and history.

Supply mappings only, not settings, aliases, or a full index-creation request.
Existing compatible source settings, including analysis definitions, are reused.
New custom analyzers must be defined in those settings; this editor does not add
analysis settings. Disabled, filtered, or synthetic `_source` in the supplied
mapping is rejected. Elasticsearch validates field definitions when creating the
new index; rejected mappings or documents stop the job and preserve the original.
Full document readback and mapping checks precede any original removal.

## Migration behavior and safety boundaries

- Migration supports matching Elasticsearch major versions: **7 → 7** (7.14+
  on both ends), **8 → 8**, and **9 → 9**, including different minor/patch
  releases. Cross-major migrations, older releases, and prerelease versions are
  rejected. Matching major versions do not guarantee that every mapping, plugin,
  or index setting is available at the destination: index creation and independent
  document verification must still succeed before any confirmed removal.
  The 7.14 minimum retains the client's product identity check and required safety
  APIs. Requests use native JSON for 7.x and the v8 REST contract for 8.x/9.x,
  following Elasticsearch's [REST API compatibility mechanism](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/compatibility).
  Source credentials must have full
  document and field visibility for selected indices; filtered security roles
  cannot establish that an entire index was copied.
- Cross-cluster migration and same-cluster reindexing are supported. Cluster UUIDs
  determine whether the connections point to the same cluster. Ordinary same-cluster copies
  use `reindex_<name>_<unique suffix>` and preserve the original index. The separate
  mapping workflow below supports editable names and explicit original removal.
- Source indices must already have an explicit write block. Stop application
  writers first, then apply `PUT /your-index/_block/write` and check its result.
  estui does not unblock sources. Keep them blocked until the entire migration
  and recovery verification finish. Disabled or filtered `_source` is rejected.
- New transfers use scroll/bulk through the configured, authenticated source
  connection. `remote_reindex_host` and `force_scroll` are retained for old
  configuration/checkpoint compatibility; new remote native tasks are disabled.
  This avoids trusting a different address from the destination's network.
- When destination names already exist on another cluster, the UI asks whether
  to create `<name>_reindex_<unique suffix>` (the default) or overwrite the same
  name. Overwrite requires an explicit data-loss acknowledgement, current cluster
  and destination index UUIDs, and a write-blocked destination. It permanently
  removes the old destination's data, configuration, and aliases. Only documents
  matching the migration query are included in the replacement.
- Overwrite first creates and independently verifies a separate staging copy.
  After checking staging health and the confirmed destination identity, it deletes
  the old destination and clones staging into the original name, already
  write-blocked. It verifies the final copy and retains staging. This is not an
  atomic cutover: failure after deletion can leave the original name unavailable.
  Green staging health and sufficient disk space for cloning are required.
- The API uses `collision_action: "rename"` or `"overwrite"`; overwrite also
  requires `source_uuid`, `target_uuid`, and `overwrite_uuids: {"name":"uuid"}`.
  A collision without a choice is rejected. The legacy `overwrite: true` field
  requests a separate copy and never authorizes deletion; explicit legacy false
  or `collision_action: "skip"` skips collisions.
- If interrupted during deletion or clone submission, Continue and Abandon stop
  for manual reconciliation; they never automatically repeat an uncertain destructive
  step. An acknowledged deletion checkpoint resumes cloning, and finalizing copies
  resume verification. Inspect the recorded staging/final names and UUIDs before
  reconciling state; do not discard the checkpoint to force a retry.
- Interrupted copies are retained. Continue starts unfinished proxy copies in
  fresh indices, preserving the partial copy and its recorded name. Cleanup is
  an explicit administrative action, not part of migration recovery.
- Aliases and lifecycle policies are withheld. Default/final ingest pipelines
  are explicitly disabled on copies. Destination templates that introduce
  aliases or lifecycle settings are rejected before creation, including component
  templates. Template inspection permissions are required; errors fail closed.
- After copying, estui write-blocks the destination, refreshes both indices, and
  independently reads every matching source document and every destination
  document. It compares counts and a 256-bit multiset fingerprint covering
  `_id`, `_routing`, and canonical JSON `_source`, preserving large JSON numbers.
  It checks index UUIDs and write blocks before and after verification.
- Only verified copies become **completed**. Destinations remain write-blocked
  and are closed; retained sources return to their original open/closed state. Restoration failures
  fail the job. Reopening a copy and enabling writes is a separate cutover step.
- Source data, query semantics, templates, index identities, and cluster routing
  must remain stable during migration. Do not run external index-management,
  lifecycle deletion/rollover, template changes, or another migration tool on
  involved indices. This is an offline copy workflow, not continuous replication.
  Use fixed query date boundaries instead of relative `now` expressions.
- The app stops at the first failure. Continue rechecks previously completed
  copies against their saved fingerprints before skipping them. It verifies
  source and destination cluster/index identities and restarts unfinished copies.
- Saved native tasks with recorded source/target identities can be monitored and
  their completed data verified. Uncertain tasks, unverifiable tasks, and old
  completed checkpoints without verification metadata require manual
  reconciliation. Abandon cannot erase an unresolved task checkpoint.
- State is saved atomically to `migration_state.json` **beside the config file**,
  independent of the launch directory. When upgrading from a run in another
  working directory, move its existing state file beside the configuration before
  starting. Keep a backup. Do not delete state to bypass unresolved tasks.
- Unix process locks prevent simultaneous instances for the same OS user or
  shared state file. Administrative mutations/previews are serialized and blocked
  while migration state is unresolved. Local processes remain trusted.
- Closed-index preview restoration is checkpointed before opening. Startup and
  subsequent mutations restore pending previews only after checking cluster and
  index UUIDs; an unrelated replacement is never closed automatically.
- Connections accept one HTTP or HTTPS endpoint, including remote HTTP hosts.
  HTTP traffic, including credentials, is unencrypted. HTTPS verifies certificates;
  embedded URL credentials and `insecure_skip_verify` are still rejected.
- The UI loads cluster identities from the backend, preserves the confirmation
  choice, checks stale cluster identities, and reports each bulk-action failure.
- HTTP retries are disabled: a lost advancing-scroll response fails the copy
  rather than silently skipping a page. Retrying happens at index level.

Only ordinary, visible indices are supported. System indices, data streams,
cluster settings, templates, users, roles, ingest pipelines and lifecycle policies
are not a complete-cluster migration or backup. Maintain tested Elasticsearch
snapshots separately. TLS, least-privilege credentials, disk capacity, and
production-version compatibility must be validated in your environment.

## Development checks

```bash
go test -race ./... -count=1
go vet ./...
node --test internal/web/ui_regression_test.cjs
go build -o /tmp/estui .
```

Default tests use mocks and temporary localhost servers, including process-kill
recovery and document-corruption detection. They do not read configured clusters.
JavaScript regression tests run from Go when Node.js is installed.

Opt-in integration tests require two **disposable** Elasticsearch clusters:

```bash
ESTUI_TEST_SOURCE=http://127.0.0.1:9200 \
ESTUI_TEST_TARGET=http://127.0.0.1:9201 \
go test ./internal/web -run '^TestElasticsearchIntegration$' -v -timeout=5m
```

These tests create, block, close/open, and clean up unique `estui-integration-*`
indices, covering new destinations, rename, staged overwrite, and same-cluster
reindexing, plus local/cross-cluster mapping changes with filtered destinations and
full original backups. Every case also reloads a saved checkpoint and uses Continue
to independently revalidate the completed copy and restore closed states. Run the
suite against a separate pair for each supported major version; it never reads
the application's configured cluster endpoints.
Optional `ESTUI_TEST_SOURCE_API_KEY`, `ESTUI_TEST_TARGET_API_KEY`,
`ESTUI_TEST_SOURCE_CA`, and `ESTUI_TEST_TARGET_CA` configure authentication/TLS.
The supplied Docker Compose file is a development fixture with security disabled;
its ports bind only to loopback. Do not use it as production configuration.

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
