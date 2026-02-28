#!/bin/bash
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

# --- CONFIGURATION ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
LOG_FILE="ingestunpli.log"

# Caricamento variabili dal file .env (per recuperare UNPLI_PROVIDER_PREFIX)
if [ -f "../.env" ]; then
    export $(grep -v '^#' ../.env | xargs)
fi

# PATH DETECTION: Allineato con la logica del backend
if [ -d "/app/dataset" ]; then
    DATASET_DIR="/app/dataset"
else
    DATASET_DIR="$SCRIPT_DIR/dataset"
fi

# PORT DETECTION: Default 8001, override tramite API_PORT=8000
API_PORT=${API_PORT:-8001}
API_URL="http://127.0.0.1:$API_PORT"

# Recupero prefisso dal .env (es. UN) o default 'UN' se vuoto
UN_PREFIX=${UNPLI_PROVIDER_PREFIX:-"UN"}

# Nomi file per la Delta Logic
CURRENT_JSON="$DATASET_DIR/unpli_current.json"
LAST_JSON="$DATASET_DIR/unpli_last.json"

echo "------------------------------------------------" | tee -a "$LOG_FILE"
echo "🚀 Started UNPLI Delta Ingestion [$UN_PREFIX]: $(date)" | tee -a "$LOG_FILE"
echo "📂 Dataset Dir: $DATASET_DIR" | tee -a "$LOG_FILE"
echo "🔗 API URL: $API_URL" | tee -a "$LOG_FILE"
echo "------------------------------------------------" | tee -a "$LOG_FILE"

# STEP 0: PREPARE
mkdir -p "$DATASET_DIR"

# Controllo flag --initialize
INITIALIZE=false
for arg in "$@"; do
  if [ "$arg" == "--initialize" ]; then
    INITIALIZE=true
  fi
done

if [ "$INITIALIZE" = true ]; then
    echo "🧹 Initialization requested. Clearing old history..." | tee -a "$LOG_FILE"
    rm -f "$LAST_JSON"
    rm -f "$CURRENT_JSON"
else
    # Rotazione: Il file corrente dell'ultimo run diventa il 'Last' per il confronto
    if [ -f "$CURRENT_JSON" ]; then
        mv "$CURRENT_JSON" "$LAST_JSON"
        echo "📦 Rotated current JSON to last_run" | tee -a "$LOG_FILE"
    fi
fi

# STEP 1: SCRAPE & PREFIXING
echo "📥 Scraping events from UNPLI and applying prefix '$UN_PREFIX'..." | tee -a "$LOG_FILE"

# Chiamata all'endpoint di scraping
SCRAPE_RESPONSE=$(curl -s -X GET \
  "$API_URL/scrape_unpli_events?page_no=1&page_size=500" \
  -H "accept: application/json")

if [ -z "$SCRAPE_RESPONSE" ]; then
    echo "❌ Error: API did not respond on port $API_PORT!" | tee -a "$LOG_FILE"
    exit 1
fi

# Trasformazione JSON: aggiunta prefisso all'ID prima del salvataggio
echo "$SCRAPE_RESPONSE" | python3 -c "
import sys, json
prefix = '$UN_PREFIX'
try:
    data = json.load(sys.stdin)
    # UNPLI restituisce un dict {'events': [...]} o una lista
    events = data.get('events', []) if isinstance(data, dict) else data
    
    # Applica il prefisso PROVIDER_ID ad ogni ID evento
    for ev in events:
        raw_id = str(ev.get('id', ''))
        if raw_id and not raw_id.startswith(prefix + '_'):
            ev['id'] = f'{prefix}_{raw_id}'
            
    print(json.dumps({'events': events}))
except Exception as e:
    sys.stderr.write(f'Error processing IDs: {e}\n')
    sys.exit(1)
" > "$CURRENT_JSON"

if [ ! -s "$CURRENT_JSON" ]; then
    echo "❌ Error: Scraper output is empty or invalid!" | tee -a "$LOG_FILE"
    exit 1
fi

# STEP 2: INGESTION
echo "🚀 Triggering Delta Computation and Ingestion..." | tee -a "$LOG_FILE"

# Chiamata all'endpoint per processare il delta (usando i file con ID prefissati)
INGEST_OUTPUT=$(curl -s -X POST "$API_URL/ingest-unpli-delta")

echo "$INGEST_OUTPUT" >> "$LOG_FILE"

# STEP 3: PARSE RESULTS
if echo "$INGEST_OUTPUT" | grep -q '"inserted"'; then
  STATS=$(echo "$INGEST_OUTPUT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    ins = d.get('inserted', 0)
    upd = d.get('updated', 0)
    del_count = d.get('deleted', 0)
    print(f'Added: {ins} | Updated: {upd} | Deleted: {del_count}')
except:
    print('Could not parse ingestion stats')
")
  echo "----------------------------------------" | tee -a "$LOG_FILE"
  echo "🎉 UNPLI SUCCESS: $STATS" | tee -a "$LOG_FILE"
  echo "----------------------------------------" | tee -a "$LOG_FILE"
else
  echo "⚠️ Warning: Ingestion failed. Check $LOG_FILE for details." | tee -a "$LOG_FILE"
fi

echo -e "✅ Pipeline Done $(date)\n" | tee -a "$LOG_FILE"