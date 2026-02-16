#!/bin/bash
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
LOG_FILE="ingesting.log"
echo "🚀 Started $(date) in $(pwd)" | tee -a "$LOG_FILE"
mkdir -p ./dataset

# STEP 1: Scrape WITHOUT -w (SIMPLE)
echo "📥 Scraping..." | tee -a "$LOG_FILE"
curl -s -X GET \
  "http://127.0.0.1:8001/scrape_unpli_events?page_no=1&page_size=500" \
  "${UNPLI_SESSION_ID:+&session_id=$UNPLI_SESSION_ID}" \
  -H "accept: application/json" | tee -a "$LOG_FILE"


# STEP 2: Wait + Find file (Look in the specific dataset directory)
echo "⏳ Waiting for JSON in $SCRIPT_DIR/dataset..." | tee -a "$LOG_FILE"
for i in {1..10}; do
  # This finds the most recent 500.json file in the dataset folder
  JSON_FILE=$(find "$SCRIPT_DIR/dataset" -name "unpli_events_*.json" -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)
  [ -n "$JSON_FILE" ] && break
  sleep 2
done

[ -z "$JSON_FILE" ] && { echo "❌ No JSON!"; ls -la ./dataset/; exit 1; }
echo "✅ $JSON_FILE ready ($(stat -c%s "$JSON_FILE" 2>/dev/null || echo "?") bytes)" | tee -a "$LOG_FILE"

# STEP 3: Ingest WITHOUT -w (SIMPLE)
echo "🚀 Ingesting..." | tee -a "$LOG_FILE"
curl -s -X POST \
  "http://127.0.0.1:8001/ingestevents" \
  -F "file=@$JSON_FILE" | tee -a "$LOG_FILE"

# STEP 4: Parse results
if grep -q '"inserted"' "$LOG_FILE"; then
  INSERTED=$(grep -o '"inserted":[^,}]*' "$LOG_FILE" | cut -d: -f2 | tr -d ' ')
  POINTS=$(grep -o '"points_count":[^,}]*' "$LOG_FILE" | cut -d: -f2 | tr -d ' ')
  echo "🎉 SUCCESS! Inserted: $INSERTED, Points: $POINTS" | tee -a "$LOG_FILE"
fi

echo "✅ Done $(date)" | tee -a "$LOG_FILE"
curl -s http://127.0.0.1:8001/collection_info | tee -a "$LOG_FILE"
