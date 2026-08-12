#!/bin/bash
set -e

SOURCE="http://localhost:9200"
TARGET="http://localhost:9201"
GREEN='\033[0;32m'
NC='\033[0m'
log() { echo -e "${GREEN}[+]${NC} $1"; }

for url in "$SOURCE" "$TARGET"; do
  curl -sf "$url/_cluster/health" > /dev/null 2>&1 || { echo "$url not ready"; exit 1; }
done

# Helper: create index + bulk insert data
seed() {
  local url=$1; local prefix=$2; local count=$3; local mapfile=$4; local replicas=${5:-0}
  python3 -c "
import json,random,sys
count = $count
prefix = '$prefix'
mapfile = '$mapfile'
replicas = $replicas
with open(f'/tmp/es_${prefix}_bulk.json', 'w') as f:
    for i in range(count):
        idx_num = random.randint(0, 24)
        idx_name = f'{prefix}-{idx_num:02d}'
        doc = {}
        exec(open(mapfile).read())
        f.write(json.dumps({'index':{'_index':idx_name}})+'\n')
        f.write(json.dumps(doc)+'\n')
print('done')
"
  curl -sf -X POST "$url/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @"/tmp/es_${prefix}_bulk.json" > /dev/null
  rm -f "/tmp/es_${prefix}_bulk.json"
  log "  $prefix: $count docs inserted across 25 indices"
}

# Mapping templates as inline files
cat > /tmp/es_map_a.py << 'PYEOF'
import datetime,random
doc = {
    'record_id': f'rec-{i:06d}',
    'source': random.choice(['api','web','mobile','batch','iot']),
    'severity': random.choice(['debug','info','warn','error','critical']),
    'message': f'log entry #{i} from service-{random.randint(1,20)}',
    'payload_size': random.randint(64, 8192),
    'duration_ms': random.randint(1, 5000),
    'status_code': random.choice([200,200,200,201,204,301,400,401,403,404,500]),
    'recorded_at': datetime.datetime(2024,1,1,0,0,0) + datetime.timedelta(
        days=random.randint(0,364), hours=random.randint(0,23), minutes=random.randint(0,59)
    ),
    'tags': random.sample(['prod','staging','critical','deprecated','beta','public','internal','pci','gdpr'], k=random.randint(1,4))
}
PYEOF

cat > /tmp/es_map_b.py << 'PYEOF'
import random
doc = {
    'user_id': random.randint(1, 100000),
    'session_id': f'sess-{random.randint(10**8, 10**9-1)}',
    'action': random.choice(['click','scroll','purchase','login','logout','signup','view_product','add_to_cart','checkout','search']),
    'page_url': f'/page/{random.choice(["home","catalog","product","cart","account","settings","help"])}/{random.randint(1,999)}',
    'referrer': random.choice(['google','facebook','twitter','direct','email','reddit']),
    'device': random.choice(['desktop','mobile','tablet']),
    'browser': random.choice(['chrome','firefox','safari','edge']),
    'load_time_ms': random.randint(50, 8000),
    'ip_hash': f'{random.randint(1,255):02x}{random.randint(0,255):02x}{random.randint(0,255):02x}{random.randint(0,255):02x}',
}
PYEOF

cat > /tmp/es_map_c.py << 'PYEOF'
import datetime,random
cats = ['gaming','music','video','sports','news','education','podcast','live','kids','fitness']
doc = {
    'stream_id': f'stream-{random.randint(10000,99999)}',
    'channel': random.choice(['ch001','ch002','ch003','ch004','ch005','ch006']),
    'category': random.choice(cats),
    'bitrate_kbps': random.choice([480,720,1080,1440,2160]),
    'duration_sec': random.randint(30, 14400),
    'viewer_count': random.randint(1, 50000),
    'encoding': random.choice(['h264','h265','vp9','av1']),
    'cdn_node': f'cdn-{random.choice(["eu","us","as","sa"])}-{random.randint(1,20):02d}',
    'started_at': datetime.datetime(2025,1,1,0,0,0) + datetime.timedelta(
        days=random.randint(0,210), hours=random.randint(0,23), minutes=random.randint(0,59)
    ),
    'metadata': {'resolution': f'{random.choice([3840,2560,1920])}x{random.choice([2160,1440,1080])}'}
}
PYEOF

cat > /tmp/es_map_d.py << 'PYEOF'
import datetime,random
doc = {
    'transaction_id': f'txn-{random.randint(10**10, 10**11-1)}',
    'account_id': f'acc-{random.randint(1000,9999)}',
    'amount': round(random.uniform(0.50, 25000), 2),
    'currency': random.choice(['USD','EUR','GBP','JPY','CAD','AUD']),
    'type': random.choice(['debit','credit','transfer','refund','chargeback']),
    'merchant': random.choice(['Amazon','Netflix','Spotify','Uber','Airbnb','Shopify','Stripe','DigitalOcean','AWS','Walmart']),
    'category': random.choice(['groceries','entertainment','transport','utilities','shopping','travel','dining']),
    'status': random.choice(['completed','pending','failed','flagged']),
    'risk_score': random.randint(0, 100),
    'processed_at': datetime.datetime(2025,1,1,0,0,0) + datetime.timedelta(
        days=random.randint(0,210), hours=random.randint(0,23), minutes=random.randint(0,59), seconds=random.randint(0,59)
    ),
}
PYEOF

# === Source indices (we already have 5, need 45 more) ===
log "Seeding source cluster ($SOURCE)..."

for i in $(seq 0 44); do
  idx="src-$(printf '%02d' "$i")"
  rep=$((i % 2))
  curl -sf -X PUT "$SOURCE/$idx" -H 'Content-Type: application/json' -d "{
    \"settings\":{\"number_of_shards\":$(((i % 2) + 1)),\"number_of_replicas\":$rep},
    \"mappings\":{\"properties\":{
      \"id\":{\"type\":\"keyword\"},
      \"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},
      \"value\":{\"type\":\"float\"},
      \"flag\":{\"type\":\"boolean\"},
      \"category\":{\"type\":\"keyword\"},
      \"created_at\":{\"type\":\"date\"}
    }}
  }" > /dev/null

  python3 -c "
import json,random
with open('/tmp/es_src_bulk.json','w') as f:
    for d in range(3, $((13 + i % 7 * 3))):
        f.write(json.dumps({'index':{'_index':'$idx'}})+'\n')
        f.write(json.dumps({
            'id':f'id-{d:04d}',
            'name':random.choice(['alpha','beta','gamma','delta','epsilon','zeta','eta','theta']),
            'value':round(random.uniform(0,100),3),
            'flag':bool(random.getrandbits(1)),
            'category':random.choice(['A','B','C','D']),
            'created_at':f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z'
        })+'\n')
"
  curl -sf -X POST "$SOURCE/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_src_bulk.json > /dev/null
  rm -f /tmp/es_src_bulk.json
done
curl -sf -X POST "$SOURCE/_refresh" > /dev/null
log "  +45 indices seeded on source"

# === Target indices (we have 3, need 47 more) ===
log "Seeding target cluster ($TARGET)..."

for i in $(seq 0 46); do
  idx="tgt-$(printf '%02d' "$i")"
  rep=$(((i + 1) % 2))
  curl -sf -X PUT "$TARGET/$idx" -H 'Content-Type: application/json' -d "{
    \"settings\":{\"number_of_shards\":$(((i % 2) + 1)),\"number_of_replicas\":$rep},
    \"mappings\":{\"properties\":{
      \"id\":{\"type\":\"keyword\"},
      \"data\":{\"type\":\"text\"},
      \"score\":{\"type\":\"float\"},
      \"active\":{\"type\":\"boolean\"},
      \"group\":{\"type\":\"keyword\"},
      \"timestamp\":{\"type\":\"date\"}
    }}
  }" > /dev/null

  python3 -c "
import json,random
with open('/tmp/es_tgt_bulk.json','w') as f:
    for d in range(2, $((10 + i % 5 * 3))):
        f.write(json.dumps({'index':{'_index':'$idx'}})+'\n')
        f.write(json.dumps({
            'id':f'id-{d:04d}',
            'data':random.choice(['lorem','ipsum','dolor','sit','amet','consectetur','adipiscing']),
            'score':round(random.uniform(0,1),4),
            'active':bool(random.getrandbits(1)),
            'group':random.choice(['X','Y','Z']),
            'timestamp':f'2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z'
        })+'\n')
"
  curl -sf -X POST "$TARGET/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_tgt_bulk.json > /dev/null
  rm -f /tmp/es_tgt_bulk.json
done
curl -sf -X POST "$TARGET/_refresh" > /dev/null
log "  +47 indices seeded on target"

sleep 1

echo ""
log "=== Source ($SOURCE) ==="
curl -sf "$SOURCE/_cat/indices?h=index,docs.count,status,pri,rep&s=index" | grep -v '^\.' | \
  awk '{printf "%-25s %6s %6s %3s %3s\n", $1, $2, $3, $4, $5}'
src_count=$(curl -sf "$SOURCE/_cat/indices?h=index&s=index" | grep -v '^\.' | wc -l)

echo ""
log "=== Target ($TARGET) ==="
curl -sf "$TARGET/_cat/indices?h=index,docs.count,status,pri,rep&s=index" | grep -v '^\.' | \
  awk '{printf "%-25s %6s %6s %3s %3s\n", $1, $2, $3, $4, $5}'
tgt_count=$(curl -sf "$TARGET/_cat/indices?h=index&s=index" | grep -v '^\.' | wc -l)

echo ""
log "Source: $src_count indices  |  Target: $tgt_count indices"
