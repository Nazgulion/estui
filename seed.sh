#!/bin/bash
set -e

SOURCE="http://localhost:9200"
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[+]${NC} $1"; }

wait_for_es() {
  local url=$1
  log "Waiting for $url..."
  until curl -sf "$url/_cluster/health" > /dev/null 2>&1; do sleep 2; done
  log "ready"
}

wait_for_es "$SOURCE"

# === users (1000 docs) ===
log "Seeding users (1000 docs)..."
curl -sf -X PUT "$SOURCE/users" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":1},
  "mappings":{"properties":{
    "username":{"type":"keyword"},"full_name":{"type":"text"},
    "email":{"type":"keyword"},"age":{"type":"integer"},
    "created_at":{"type":"date"},"is_active":{"type":"boolean"},
    "tags":{"type":"keyword"}
  }}
}' > /dev/null

python3 -c "
import json,random,datetime
first=['Alice','Bob','Charlie','Diana','Eve','Frank','Grace','Hank','Ivy','Jack','Kate','Leo','Mona','Nate','Olga','Pete','Quinn','Rose','Sam','Tina']
last=['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez']
tags=['admin','editor','viewer','api-user','beta','vip','premium','trial','partner','moderator']
with open('/tmp/es_bulk.json','w') as f:
    for i in range(1,1001):
        t=[random.choice(tags) for _ in range(random.randint(1,3))]
        f.write(json.dumps({'index':{'_index':'users'}})+'\n')
        f.write(json.dumps({'username':f'user{i}','full_name':f'{random.choice(first)} {random.choice(last)}','email':f'user{i}@ex.com','age':random.randint(18,73),'created_at':datetime.datetime(2024,1,1).isoformat(),'is_active':bool(random.getrandbits(1)),'tags':t})+'\n')
"
curl -sf -X POST "$SOURCE/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_bulk.json > /dev/null
rm /tmp/es_bulk.json

# === products (500 docs) ===
log "Seeding products (500 docs)..."
curl -sf -X PUT "$SOURCE/products" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":1},
  "mappings":{"properties":{
    "name":{"type":"text","fields":{"keyword":{"type":"keyword"}}},
    "description":{"type":"text"},"price":{"type":"float"},
    "category":{"type":"keyword"},"in_stock":{"type":"boolean"},
    "rating":{"type":"float"},"created_at":{"type":"date"}
  }}
}' > /dev/null

python3 -c "
import json,random
cats=['electronics','books','clothing','food','sports']
names=['Laptop','Smartphone','Headphones','Monitor','Keyboard','Mouse','Tablet','Watch','Camera','Speaker','Novel','Cookbook','Journal','Textbook','T-Shirt','Jeans','Jacket','Sneakers','Hat','ProteinBar','Coffee','Tea','Vitamins','OliveOil','Racket','YogaMat','Dumbbells','Bicycle','Helmet','Charger','Cable','USBHub','SDCard','PowerBank','DeskLamp','Backpack','Wallet','Sunglasses','Umbrella','Soap','Shampoo','Towels','Candle','Frame','Plant','Mug','Plate','Scissors','Glue']
with open('/tmp/es_bulk.json','w') as f:
    for i in range(1,501):
        n=random.choice(names)
        f.write(json.dumps({'index':{'_index':'products'}})+'\n')
        f.write(json.dumps({'name':f'{n} #{i}','description':f'Quality {n} for everyday use','price':round(random.uniform(5,500),2),'category':random.choice(cats),'in_stock':bool(random.getrandbits(1)),'rating':round(random.uniform(1,5),1),'created_at':f'202{random.randint(2,5)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z'})+'\n')
"
curl -sf -X POST "$SOURCE/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_bulk.json > /dev/null
rm /tmp/es_bulk.json

# === orders-2024 (500 docs) ===
log "Seeding orders-2024 (500 docs)..."
curl -sf -X PUT "$SOURCE/orders-2024" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":2,"number_of_replicas":1},
  "mappings":{"properties":{
    "order_id":{"type":"keyword"},"user_id":{"type":"integer"},
    "product_id":{"type":"integer"},"amount":{"type":"float"},
    "status":{"type":"keyword"},"created_at":{"type":"date"},
    "tags":{"type":"keyword"}
  }}
}' > /dev/null

python3 -c "
import json,random
sts=['pending','processing','shipped','delivered','cancelled','refunded']
with open('/tmp/es_bulk.json','w') as f:
    for i in range(1,501):
        s=random.choice(sts)
        t='problematic' if s in ('cancelled','refunded') else 'normal'
        f.write(json.dumps({'index':{'_index':'orders-2024'}})+'\n')
        f.write(json.dumps({'order_id':f'ORD-{i:06d}','user_id':random.randint(1,1000),'product_id':random.randint(1,500),'amount':round(random.uniform(10,1000),2),'status':s,'created_at':f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z','tags':[t]})+'\n')
"
curl -sf -X POST "$SOURCE/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_bulk.json > /dev/null
rm /tmp/es_bulk.json

# === orders-2025 (200 docs) ===
log "Seeding orders-2025 (200 docs)..."
curl -sf -X PUT "$SOURCE/orders-2025" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":2,"number_of_replicas":1},
  "mappings":{"properties":{
    "order_id":{"type":"keyword"},"user_id":{"type":"integer"},
    "product_id":{"type":"integer"},"amount":{"type":"float"},
    "status":{"type":"keyword"},"created_at":{"type":"date"},
    "tags":{"type":"keyword"}
  }}
}' > /dev/null

python3 -c "
import json,random
sts=['pending','processing','shipped','delivered','cancelled','refunded']
with open('/tmp/es_bulk.json','w') as f:
    for i in range(1,201):
        s=random.choice(sts)
        t='problematic' if s in ('cancelled','refunded') else 'normal'
        f.write(json.dumps({'index':{'_index':'orders-2025'}})+'\n')
        f.write(json.dumps({'order_id':f'ORD2025-{i:06d}','user_id':random.randint(1,1000),'product_id':random.randint(1,500),'amount':round(random.uniform(10,1000),2),'status':s,'created_at':f'2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z','tags':[t]})+'\n')
"
curl -sf -X POST "$SOURCE/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_bulk.json > /dev/null
rm /tmp/es_bulk.json

# === audit-logs-archived (50 docs, CLOSED) ===
log "Seeding audit-logs-archived (50 docs, then closing)..."
curl -sf -X PUT "$SOURCE/audit-logs-archived" -H 'Content-Type: application/json' -d '{
  "settings":{"number_of_shards":1,"number_of_replicas":0},
  "mappings":{"properties":{
    "event_type":{"type":"keyword"},"user":{"type":"keyword"},
    "message":{"type":"text"},"timestamp":{"type":"date"},
    "severity":{"type":"keyword"}
  }}
}' > /dev/null

python3 -c "
import json,random
ev=['LOGIN','LOGOUT','PASSWORD_CHANGE','PROFILE_UPDATE','ADMIN_ACTION']
sv=['low','medium','high']
with open('/tmp/es_bulk.json','w') as f:
    for i in range(1,51):
        e=random.choice(ev)
        f.write(json.dumps({'index':{'_index':'audit-logs-archived'}})+'\n')
        f.write(json.dumps({'event_type':e,'user':f'user{random.randint(1,1000)}','message':f'{e} event #{i}','timestamp':f'2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z','severity':random.choice(sv)})+'\n')
"
curl -sf -X POST "$SOURCE/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/es_bulk.json > /dev/null
rm /tmp/es_bulk.json
curl -sf -X POST "$SOURCE/audit-logs-archived/_close" > /dev/null

# === refresh and summary ===
curl -sf -X POST "$SOURCE/_refresh" > /dev/null
sleep 1

echo ""
log "=== Source indices ==="
curl -sf "$SOURCE/_cat/indices?h=index,status,health,docs.count,pri,rep,store.size&s=index" | grep -v '^\.'

echo ""
log "=== Target indices (empty) ==="
curl -sf "$SOURCE:9201/_cat/indices?h=index&s=index" 2>/dev/null || echo "(no indices)"

echo ""
log "Run: ./estui"
