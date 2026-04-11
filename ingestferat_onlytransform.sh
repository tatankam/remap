#!/bin/bash

# —————————————————————————————————————————————————————————————————————
# Feratel DSI Ingestion Script (Enhanced with Initialize Mode)
# —————————————————————————————————————————————————————————————————————

# 1. Load Configuration
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ ERROR: .env file not found."
    exit 1
fi

# Configurazione Porte e URL
API_PORT=${API_PORT:-8001}
API_URL="http://127.0.0.1:$API_PORT"
INGEST_ENDPOINT="$API_URL/ingest-feratel"

DATASET_DIR="./dataset"
LOG_DIR="logs"
mkdir -p "$DATASET_DIR" "$LOG_DIR"
LOG_FILE="$LOG_DIR/feratel_ingest_$(date +%Y-%m-%d).log"

# Paths dei file
TEMPLATE_FILE="${DATASET_DIR}/soap_request.xml.template"
ACTIVE_REQUEST="${DATASET_DIR}/soap_request.xml"
REQ_KEYVALUES="${DATASET_DIR}/keyvalues_request.xml"
LAST_INGEST_FILE="${DATASET_DIR}/feratel_std_last.json"

# File di Output (Sovrascritti ad ogni esecuzione)
RAW_EVENTS="${DATASET_DIR}/feratel_raw_events.xml"
RAW_KEYVALUES="${DATASET_DIR}/feratel_raw_keyvalues.xml"

echo "[$(date)] 🚀 Starting Feratel Fetch..." | tee -a "$LOG_FILE"

# —————————————————————————————————————————————————————————————————————
# 2. Handle Initialize Flag
# —————————————————————————————————————————————————————————————————————
if [[ "$1" == "--initialize" ]]; then
    echo "⚠️  Initialize mode detected: Forcing full re-ingestion..." | tee -a "$LOG_FILE"
    if [ -f "$LAST_INGEST_FILE" ]; then
        rm "$LAST_INGEST_FILE"
        echo "🗑️  Removed $LAST_INGEST_FILE to bypass backend skip logic." | tee -a "$LOG_FILE"
    else
        echo "ℹ️  No previous state file found. Proceeding normally." | tee -a "$LOG_FILE"
    fi
fi

# —————————————————————————————————————————————————————————————————————
# 3. Generate Request from Template
# —————————————————————————————————————————————————————————————————————
TODAY=$(date +%Y-%m-%d)
if [ -f "$TEMPLATE_FILE" ]; then
    sed -e "s/{{START_DATE}}/$TODAY/g" \
        -e "s/{{COMPANY}}/$FERATEL_COMPANY/g" \
        -e "s/{{ITEM_ID}}/$FERATEL_ITEM_ID/g" \
        "$TEMPLATE_FILE" > "$ACTIVE_REQUEST"
else
    echo "❌ ERROR: Template file $TEMPLATE_FILE not found." | tee -a "$LOG_FILE"
    exit 1
fi

# —————————————————————————————————————————————————————————————————————
# 4. Fetch Raw Data from Feratel
# —————————————————————————————————————————————————————————————————————
# echo "📥 Downloading Events..." | tee -a "$LOG_FILE"
# curl -s -X POST -H "Content-Type: text/xml; charset=utf-8" \
#      -H 'SOAPAction: "http://tempuri.org/GetData"' \
#      --data-binary @"${ACTIVE_REQUEST}" \
#      'http://interface.deskline.net/DSI/BasicData.asmx' > "${RAW_EVENTS}"

# echo "📥 Downloading KeyValues..." | tee -a "$LOG_FILE"
# curl -s -X POST -H "Content-Type: text/xml; charset=utf-8" \
#      -H "SOAPAction: http://tempuri.org/GetKeyValues" \
#      --data-binary @"${REQ_KEYVALUES}" \
#      'http://interface.deskline.net/DSI/KeyValue.asmx' > "${RAW_KEYVALUES}"

# —————————————————————————————————————————————————————————————————————
# 5. Trigger Backend Processing
# —————————————————————————————————————————————————————————————————————
echo "⚙️  Triggering Backend Processing at $INGEST_ENDPOINT..." | tee -a "$LOG_FILE"
RESPONSE=$(curl -s -X POST "${INGEST_ENDPOINT}")

# Output finale
echo "[$(date)] ✅ Finished. Server said: $RESPONSE" | tee -a "$LOG_FILE"