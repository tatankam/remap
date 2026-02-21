#!/bin/bash
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

# --- CONFIGURATION ---
# Setup directory and logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
LOG_FILE="ingestunpli.log"

# PATH DETECTION: Align with ingest_service.py logic
if [ -d "/app/dataset" ]; then
    DATASET_DIR="/app/dataset"
else
    DATASET_DIR="$SCRIPT_DIR/dataset"
fi

# PORT DETECTION: Default to 8001 (Docker), allow override via API_PORT=8000
API_PORT=${API_PORT:-8001}
API_URL="http://127.0.0.1:$API_PORT"

# File names for the Delta Logic
CURRENT_JSON="$DATASET_DIR/unpli_current.json"
LAST_JSON="$DATASET_DIR/unpli_last.json"

echo "------------------------------------------------" | tee -a "$LOG_FILE"
echo "🚀 Started UNPLI Delta Ingestion: $(date)" | tee -a "$LOG_FILE"
echo "📂 Dataset Dir: $DATASET_DIR" | tee -a "$LOG_FILE"
echo "🔗 API URL: $API_URL" | tee -a "$LOG_FILE"
echo "------------------------------------------------" | tee -a "$LOG_FILE"

# STEP 0: PREPARE
mkdir -p "$DATASET_DIR"

# Check for --initialize flag
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
    # Rotate: Today becomes the "Last" for comparison
    if [ -f "$CURRENT_JSON" ]; then
        mv "$CURRENT_JSON" "$LAST_JSON"
        echo "📦 Rotated current JSON to last_run" | tee -a "$LOG_FILE"
    fi
fi

# STEP 1: SCRAPE
echo "📥 Scraping 500 events from UNPLI..." | tee -a "$LOG_FILE"

# Call the endpoint and capture the full JSON response
SCRAPE_RESPONSE=$(curl -s -X GET \
  "$API_URL/scrape_unpli_events?page_no=1&page_size=500" \
  -H "accept: application/json")

if [ -z "$SCRAPE_RESPONSE" ]; then
    echo "❌ Error: API did not respond on port $API_PORT!" | tee -a "$LOG_FILE"
    exit 1
fi

# Extract the events list and save directly to CURRENT_JSON
echo "$SCRAPE_RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    # Ensure structure is {'events': [...]}
    if isinstance(data, dict) and 'events' in data:
        output = {'events': data['events']}
    else:
        output = {'events': data}
    print(json.dumps(output))
except Exception as e:
    sys.stderr.write(f'Error parsing JSON: {e}\n')
    sys.exit(1)
" > "$CURRENT_JSON"

if [ ! -s "$CURRENT_JSON" ]; then
    echo "❌ Error: Scraper output is empty or invalid!" | tee -a "$LOG_FILE"
    exit 1
fi

# DEBUG: Count unique IDs
UNIQUE_IDS=$(grep -o '"id": "[^"]*"' "$CURRENT_JSON" | sort | uniq | wc -l)
echo "🔍 Found $UNIQUE_IDS unique event IDs in current scrape." | tee -a "$LOG_FILE"

# STEP 2: INGESTION
echo "🚀 Triggering Delta Computation and Ingestion..." | tee -a "$LOG_FILE"

# Call the ingest-unpli-delta endpoint
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