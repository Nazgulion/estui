#!/bin/bash
set -e

TARGET="http://localhost:9201"
GREEN='\033[0;32m'
NC='\033[0m'
log() { echo -e "${GREEN}[+]${NC} $1"; }

curl -sf "$TARGET/_cluster/health" > /dev/null 2>&1 || { echo "target not ready"; exit 1; }

# === events (300 docs) ===
log "Seeding events (300 docs)..."
curl -sf -X PUT "$TARGET/events" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0},
  "mappings":{"properties":{
    "event":{"type":"keyword"},"user_id":{"type":"integer"},
    "payload":{"type":"text"},"recorded_at":{"type":"date"}
  }}
}' > /dev/null

python3 -c "
import json,random
events=['click','view','scroll','submit','purchase','login','logout','signup','search','share']
with open('/tmp/es_tgt_bulk.json','w') as f:
    for i in range(1,301):
        e=random.choice(events)
        f.write(json.dumps({'index':{'_index':'events'}})+'\n')
        f.write(json.dumps({'event':e,'user_id':random.randint(1,500),'payload':f'payload-{i}-{e}','recorded_at':f'2025-0{random.randint(1,9)}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z'})+'\n')
"
curl -sf -X POST "$TARGET/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_tgt_bulk.json > /dev/null
rm /tmp/es_tgt_bulk.json

# === sensors (200 docs) ===
log "Seeding sensors (200 docs)..."
curl -sf -X PUT "$TARGET/sensors" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0},
  "mappings":{"properties":{
    "sensor_id":{"type":"keyword"},"location":{"type":"geo_point"},
    "temperature":{"type":"float"},"humidity":{"type":"float"},
    "battery":{"type":"float"},"reported_at":{"type":"date"}
  }}
}' > /dev/null

python3 -c "
import json,random
with open('/tmp/es_tgt_bulk.json','w') as f:
    for i in range(1,201):
        f.write(json.dumps({'index':{'_index':'sensors'}})+'\n')
        f.write(json.dumps({'sensor_id':f'sensor-{i:04d}','location':{'lat':round(random.uniform(40,50),4),'lon':round(random.uniform(-5,15),4)},'temperature':round(random.uniform(-10,45),2),'humidity':round(random.uniform(20,95),1),'battery':round(random.uniform(1.5,5.0),2),'reported_at':f'2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z'})+'\n')
"
curl -sf -X POST "$TARGET/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_tgt_bulk.json > /dev/null
rm /tmp/es_tgt_bulk.json

# === cache-data (closed, 100 docs) ===
log "Seeding cache-data (100 docs, then closing)..."
curl -sf -X PUT "$TARGET/cache-data" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0},
  "mappings":{"properties":{
    "key":{"type":"keyword"},"value":{"type":"text"},
    "ttl_seconds":{"type":"integer"},"cached_at":{"type":"date"}
  }}
}' > /dev/null

python3 -c "
import json,random
with open('/tmp/es_tgt_bulk.json','w') as f:
    for i in range(1,101):
        f.write(json.dumps({'index':{'_index':'cache-data'}})+'\n')
        f.write(json.dumps({'key':f'cache-key-{i}','value':f'some-expired-value-{i}','ttl_seconds':random.choice([60,300,3600,86400]),'cached_at':f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z'})+'\n')
"
curl -sf -X POST "$TARGET/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_tgt_bulk.json > /dev/null
rm /tmp/es_tgt_bulk.json
curl -sf -X POST "$TARGET/cache-data/_close" > /dev/null

curl -sf -X POST "$TARGET/_refresh" > /dev/null
sleep 1

echo ""
log "=== Target indices ==="
curl -sf "$TARGET/_cat/indices?h=index,status,health,docs.count,pri,rep,store.size&s=index" | grep -v '^\.'
