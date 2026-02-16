#!/bin/bash
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

# Setup directory e logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
LOG_FILE="ingesting.log"
DATASET_DIR="$SCRIPT_DIR/dataset"

echo "🚀 Started $(date) in $(pwd)" | tee -a "$LOG_FILE"

# STEP 0: PREPARAZIONE E PULIZIA
# Creiamo la cartella se non esiste e rimuoviamo vecchi JSON per evitare falsi positivi
mkdir -p "$DATASET_DIR"
echo "🧹 Cleaning up previous JSON files in $DATASET_DIR..." | tee -a "$LOG_FILE"
rm -f "$DATASET_DIR"/unpli_events_*.json

# STEP 1: SCRAPE
# Richiede lo scraping al backend (backend2 su porta 8001)
echo "📥 Scraping 500 events from UNPLI..." | tee -a "$LOG_FILE"
curl -s -X GET \
  "http://127.0.0.1:8000/scrape_unpli_events?page_no=1&page_size=500" \
  "${UNPLI_SESSION_ID:+&session_id=$UNPLI_SESSION_ID}" \
  -H "accept: application/json" | tee -a "$LOG_FILE"
echo "" >> "$LOG_FILE"

# STEP 2: ATTESA E RILEVAMENTO FILE
# Lo scraper scrive su disco tramite il volume Docker; attendiamo che il file appaia sull'host
echo "⏳ Waiting for new JSON in $DATASET_DIR..." | tee -a "$LOG_FILE"
JSON_FILE=""
for i in {1..15}; do
  # Trova il file più recente che inizia con unpli_events_
  FOUND=$(find "$DATASET_DIR" -name "unpli_events_*.json" -type f -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)
  
  if [ -n "$FOUND" ] && [ -s "$FOUND" ]; then
    JSON_FILE="$FOUND"
    break
  fi
  sleep 2
done

if [ -z "$JSON_FILE" ]; then
  echo "❌ Error: No new JSON file found! Scraper might have failed." | tee -a "$LOG_FILE"
  exit 1
fi

echo "✅ $JSON_FILE ready ($(stat -c%s "$JSON_FILE" 2>/dev/null || echo "?") bytes)" | tee -a "$LOG_FILE"

# STEP 3: INGESTION
# Invia il file rilevato al backend per l'elaborazione dei vettori e l'upload su Qdrant
echo "🚀 Ingesting into Qdrant..." | tee -a "$LOG_FILE"
INGEST_OUTPUT=$(curl -s -X POST \
  "http://127.0.0.1:8000/ingestevents" \
  -F "file=@$JSON_FILE")

# Registriamo l'output nel log per il parsing
echo "$INGEST_OUTPUT" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

# STEP 4: PARSE RESULTS & STATS
# Estraiamo i dati dall'output JSON dell'ingestione
if echo "$INGEST_OUTPUT" | grep -q '"inserted"'; then
  INSERTED=$(echo "$INGEST_OUTPUT" | grep -o '"inserted":[^,}]*' | cut -d: -f2 | tr -d ' ')
  UPDATED=$(echo "$INGEST_OUTPUT" | grep -o '"updated":[^,}]*' | cut -d: -f2 | tr -d ' ')
  SKIPPED=$(echo "$INGEST_OUTPUT" | grep -o '"skipped_unchanged":[^,}]*' | cut -d: -f2 | tr -d ' ')
  
  echo "----------------------------------------" | tee -a "$LOG_FILE"
  echo "🎉 SUCCESSFUL INGESTION" | tee -a "$LOG_FILE"
  echo "➕ New Events:    $INSERTED" | tee -a "$LOG_FILE"
  echo "🔄 Updated:       $UPDATED" | tee -a "$LOG_FILE"
  echo "⏭️ Unchanged:     $SKIPPED" | tee -a "$LOG_FILE"
  echo "----------------------------------------" | tee -a "$LOG_FILE"
else
  echo "⚠️ Warning: Ingestion response was unexpected. Check $LOG_FILE" | tee -a "$LOG_FILE"
fi

# STEP 5: CONCLUSIONE E INFO COLLEZIONE
echo "📊 Final Collection Info:" | tee -a "$LOG_FILE"
curl -s http://127.0.0.1:8000/collection_info | tee -a "$LOG_FILE"
echo -e "\n✅ Pipeline Done $(date)\n" | tee -a "$LOG_FILE"