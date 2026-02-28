#!/bin/bash
export PATH=/usr/bin:/bin:/usr/sbin:/sbin

# --- CONFIGURATION ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 1. Carica variabili dal file .env (TM_API_KEY, TM_API_FEED_URL)
if [ -f .env ]; then
    source .env
else
    echo "❌ Error: .env file not found!"
    exit 1
fi



# --- CONFIGURATION UPDATE ---
# Prende il primo argomento come COUNTRY, se vuoto usa IT
COUNTRY=${1:-"IT"}
shift # Rimuove il primo argomento così $@ contiene solo i flag come --initialize

# 2. PORT DETECTION: Default 8001 (Docker), override via API_PORT=8000 ./ingestticketmaster.sh
API_PORT=${API_PORT:-8001}
API_URL="http://127.0.0.1:$API_PORT"



COUNTRY="IT"
LOG_FILE="ingesttm_${COUNTRY}.log"

# Rilevamento cartella dataset
if [ -d "/app/dataset" ]; then
    DATASET_DIR="/app/dataset"
else
    DATASET_DIR="$SCRIPT_DIR/dataset"
fi

TODAY=$(date +%Y-%m-%d)

# File names
GZ_FILE="$DATASET_DIR/tm_event_${COUNTRY}_$TODAY.json.gz"
CURRENT_JSON="$DATASET_DIR/tm_current_$COUNTRY.json"
LAST_JSON="$DATASET_DIR/tm_last_$COUNTRY.json"

echo "------------------------------------------------" | tee -a "$LOG_FILE"
echo "🚀 Started TM Feed Ingestion [$COUNTRY]: $(date)" | tee -a "$LOG_FILE"
echo "🔗 API URL: $API_URL" | tee -a "$LOG_FILE"
echo "------------------------------------------------" | tee -a "$LOG_FILE"

mkdir -p "$DATASET_DIR"

# --- STEP 0: ROTATION / INITIALIZE ---
INITIALIZE=false
for arg in "$@"; do
  if [ "$arg" == "--initialize" ]; then 
    INITIALIZE=true 
  fi
done

if [ "$INITIALIZE" = true ]; then
    echo "🧹 Initialization requested. Clearing $COUNTRY history..." | tee -a "$LOG_FILE"
    rm -f "$LAST_JSON" "$CURRENT_JSON"
    # Pulizia file standardizzati per forzare ricalcolo delta totale in Python
    rm -f "$DATASET_DIR/tm_std_${COUNTRY}_last.json"
    rm -f "$DATASET_DIR/tm_std_${COUNTRY}_current.json"
else
    if [ -f "$CURRENT_JSON" ]; then
        mv "$CURRENT_JSON" "$LAST_JSON"
        echo "📦 Rotated $COUNTRY JSON to last_run" | tee -a "$LOG_FILE"
    fi
fi

# --- STEP 1: DOWNLOAD ---
echo "📥 Downloading $COUNTRY Feed from Ticketmaster..." | tee -a "$LOG_FILE"
curl -s -L -f "$TM_API_FEED_URL?apikey=$TM_API_KEY&countryCode=$COUNTRY" -o "$GZ_FILE"

if [ $? -ne 0 ]; then
    echo "❌ Error: Download failed! Check API Key or URL in .env" | tee -a "$LOG_FILE"
    exit 1
fi

# --- STEP 2: EXTRACT ---
echo "📂 Extracting to $CURRENT_JSON..." | tee -a "$LOG_FILE"
gunzip -c "$GZ_FILE" > "$CURRENT_JSON"
rm "$GZ_FILE" 

if [ ! -s "$CURRENT_JSON" ]; then
    echo "❌ Error: Extracted file is empty!" | tee -a "$LOG_FILE"
    exit 1
fi

# --- STEP 3: INGESTION ---
echo "🚀 Triggering Delta Computation and Ingestion..." | tee -a "$LOG_FILE"
INGEST_OUTPUT=$(curl -s -X POST "$API_URL/ingest-tm-delta?country=$COUNTRY")

echo "📥 API Response: $INGEST_OUTPUT" >> "$LOG_FILE"

if echo "$INGEST_OUTPUT" | grep -q '"status":"success"'; then
  # Parsing veloce per i log
  STATS=$(echo "$INGEST_OUTPUT" | grep -oE '"delta_applied":[0-9]+' | cut -d: -f2)
  echo "----------------------------------------" | tee -a "$LOG_FILE"
  echo "🎉 TM SUCCESS: Delta Applied: $STATS" | tee -a "$LOG_FILE"
  echo "----------------------------------------" | tee -a "$LOG_FILE"
else
  echo "⚠️ Warning: Ingestion failed. Check $LOG_FILE" | tee -a "$LOG_FILE"
fi

echo -e "✅ Pipeline Done $(date)\n" | tee -a "$LOG_FILE"