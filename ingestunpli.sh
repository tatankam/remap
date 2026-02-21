#!/bin/bash
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

# Setup directory and logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
LOG_FILE="ingestunpli.log"
DATASET_DIR="$SCRIPT_DIR/dataset"

# Fixed names for the Delta Service to recognize
CURRENT_JSON="$DATASET_DIR/unpli_current.json"
LAST_JSON="$DATASET_DIR/unpli_last.json"

echo "🚀 Started UNPLI Delta Ingestion: $(date)" | tee -a "$LOG_FILE"

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

# We call the endpoint and capture the full JSON response
SCRAPE_RESPONSE=$(curl -s -X GET \
  "http://127.0.0.1:8001/scrape_unpli_events?page_no=1&page_size=500" \
  -H "accept: application/json")

# Extract the events list and save directly to CURRENT_JSON
# This ensures we have the correct structure for the delta service
echo "$SCRAPE_RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    # The route returns {'events': [...]}
    if 'events' in data:
        output = {'events': data['events']}
    else:
        # Fallback if the structure is different
        output = {'events': data}
    print(json.dumps(output))
except Exception as e:
    sys.stderr.write(f'Error parsing JSON: {e}')
    sys.exit(1)
" > "$CURRENT_JSON"

if [ ! -s "$CURRENT_JSON" ]; then
    echo "❌ Error: Scraper returned empty or invalid file!" | tee -a "$LOG_FILE"
    exit 1
fi

# DEBUG: Count unique IDs in the file to explain the Qdrant count
UNIQUE_IDS=$(grep -o '"id": "[^"]*"' "$CURRENT_JSON" | sort | uniq | wc -l)
echo "🔍 Found $UNIQUE_IDS unique event IDs in current scrape." | tee -a "$LOG_FILE"

# STEP 2: INGESTION
echo "🚀 Triggering Delta Computation and Ingestion..." | tee -a "$LOG_FILE"

# Call the ingest-unpli-delta endpoint
# No payload sent because the service reads CURRENT_JSON and LAST_JSON from disk
INGEST_OUTPUT=$(curl -s -X POST "http://127.0.0.1:8001/ingest-unpli-delta")

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
  echo "⚠️ Warning: Ingestion failed or returned unexpected data. Check $LOG_FILE" | tee -a "$LOG_FILE"
fi

echo -e "✅ Pipeline Done $(date)\n" | tee -a "$LOG_FILE"