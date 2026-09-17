#!/bin/bash
#
# seed_indices.sh — create 50 test indices (8 groups of 5-10, each group with a
# different naming convention) plus sample docs on BOTH ES nodes, so migration
# can be tested in either direction.
#
# Usage:
#   ./seed_indices.sh                 create on source + target (skip existing)
#   ./seed_indices.sh --recreate      delete managed indices first, then recreate fresh
#   ./seed_indices.sh --source-only   only on es-source (localhost:9200)
#   ./seed_indices.sh --target-only   only on es-target (localhost:9201)
#
set -euo pipefail

SOURCE="http://localhost:9200"
TARGET="http://localhost:9201"
DOCS_PER_INDEX=10

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'
log()  { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }

RECREATE=0
NODES=()
for arg in "$@"; do
  case "$arg" in
    --recreate)    RECREATE=1 ;;
    --source-only) NODES=("$SOURCE") ;;
    --target-only) NODES=("$TARGET") ;;
    -h|--help)     sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 1 ;;
  esac
done
[ ${#NODES[@]} -eq 0 ] && NODES=("$SOURCE" "$TARGET")

# ---------------------------------------------------------------------------
# Index manifest: "<group>\t<index>" — 8 naming conventions, 50 indices total
# ---------------------------------------------------------------------------
manifest() {
  local d n m i
  # 1) daily date suffix: logs-YYYY.MM.DD (7)
  for d in 01 02 03 04 05 06 07; do echo -e "logs-daily\tlogs-2024.01.$d"; done
  # 2) zero-padded sequence: metrics-000NNN (6)
  for n in 1 2 3 4 5 6; do echo -e "metrics-seq\tmetrics-00000$n"; done
  # 3) year-month suffix: app-events-YYYY-MM (5)
  for m in 01 02 03 04 05; do echo -e "app-events-monthly\tapp-events-2024-$m"; done
  # 4) env-service-instance (8)
  for i in prod-api-1 prod-api-2 prod-web-1 prod-web-2 prod-worker-1 prod-worker-2 staging-api-1 staging-web-1; do
    echo -e "env-service\t$i"
  done
  # 5) underscore + YYYYMM: events_2024MM (6)
  for m in 01 02 03 04 05 06; do echo -e "events-underscore\tevents_2024$m"; done
  # 6) tenant prefix + year.month (5)
  for i in acme-2024.01 globex-2024.02 initech-2024.03 umbrella-2024.04 hooli-2024.05; do
    echo -e "tenant-ym\ttenant-$i"
  done
  # 7) versioned names: <name>-vN (8)
  for i in users-v1 products-v1 orders-v2 inventory-v1 payments-v3 customers-v2 catalog-v1 sessions-v1; do
    echo -e "versioned\t$i"
  done
  # 8) quarterly archive, underscore: archive_YYYY_qN (5)
  for i in archive_2023_q1 archive_2023_q2 archive_2023_q3 archive_2023_q4 archive_2024_q1; do
    echo -e "archive-quarterly\t$i"
  done
}

wait_for_es() {
  local url=$1
  log "Waiting for $url..."
  until curl -sf "$url/_cluster/health" > /dev/null 2>&1; do sleep 2; done
  log "$url ready"
}

index_exists() { # host index
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$1/$2")" = "200" ]
}

create_index() { # host index
  curl -sf -X PUT "$1/$2" -H 'Content-Type: application/json' -d '{
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {"properties": {
      "seq":        {"type": "long"},
      "origin":     {"type": "keyword"},
      "group":      {"type": "keyword"},
      "message":    {"type": "text"},
      "value":      {"type": "double"},
      "created_at": {"type": "date"},
      "tags":       {"type": "keyword"}
    }}
  }' > /dev/null
}

seed_docs() { # host origin manifest_file
  local host=$1 origin=$2 mfile=$3 bulk
  bulk=$(mktemp)
  python3 - "$origin" "$mfile" "$DOCS_PER_INDEX" > "$bulk" <<'PY'
import json, sys, random
origin, mfile, per = sys.argv[1], sys.argv[2], int(sys.argv[3])
random.seed(origin)
for line in open(mfile):
    group, idx = line.rstrip("\n").split("\t")
    for i in range(1, per + 1):
        print(json.dumps({"index": {"_index": idx, "_id": f"{origin}-{idx}-{i}"}}))
        print(json.dumps({
            "seq": i, "origin": origin, "group": group,
            "message": f"doc #{i} in {idx} ({origin})",
            "value": round(random.uniform(1, 1000), 2),
            "created_at": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z",
            "tags": [group, origin],
        }))
PY
  curl -sf -X POST "$host/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @"$bulk" > /dev/null
  rm -f "$bulk"
}

node_origin() { [ "$1" = "$SOURCE" ] && echo source || echo target; }

# ---------------------------------------------------------------------------
MFILE=$(mktemp)
manifest > "$MFILE"
TOTAL=$(wc -l < "$MFILE")
log "Manifest: $TOTAL indices in $(cut -f1 "$MFILE" | sort -u | wc -l) naming-convention groups"

for node in "${NODES[@]}"; do
  origin=$(node_origin "$node")
  wait_for_es "$node"

  if [ "$RECREATE" = 1 ]; then
    log "[$origin] deleting managed indices..."
    while IFS=$'\t' read -r _ idx; do
      curl -s -o /dev/null -X DELETE "$node/$idx" || true
    done < "$MFILE"
  fi

  log "[$origin] creating indices..."
  created=0; skipped=0
  while IFS=$'\t' read -r _ idx; do
    if index_exists "$node" "$idx"; then
      skipped=$((skipped + 1))
    else
      create_index "$node" "$idx"
      created=$((created + 1))
    fi
  done < "$MFILE"
  log "[$origin] $created created, $skipped already existed"

  log "[$origin] seeding $DOCS_PER_INDEX docs per index (deterministic ids, re-runnable)..."
  seed_docs "$node" "$origin" "$MFILE"
  curl -sf -X POST "$node/_refresh" > /dev/null
done
rm -f "$MFILE"

for node in "${NODES[@]}"; do
  echo ""
  log "=== $(node_origin "$node") ($node) ==="
  curl -sf "$node/_cat/indices?h=index,status,health,docs.count,store.size&s=index" | grep -v '^\.'
  echo "total: $(curl -sf "$node/_cat/indices?h=index&s=index" | grep -vc '^\.') indices"
done
