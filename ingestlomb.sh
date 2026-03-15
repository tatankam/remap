#!/bin/bash

# --- CONFIGURATION ---
# 1. LOAD CONFIGURATION (Optional: for internal logging if needed)
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# 2. PORT DETECTION: Default 8001 (Docker), override via API_PORT=8000 ./ingestlomb.sh
API_PORT=${API_PORT:-8001}
API_URL="http://127.0.0.1:$API_PORT"
INGEST_ENDPOINT="$API_URL/ingest-lombardia-delta"

LOG_DIR="logs"
LOG_FILE="$LOG_DIR/lombardia_ingest_$(date +%Y-%m-%d).log"

# Crea la directory dei log se non esiste
mkdir -p "$LOG_DIR"

# --- INITIALIZATION CHECK ---
INIT_PARAM=""
if [[ "$1" == "--initialize" ]]; then
    echo "[$(date)] ⚠️  MODALITÀ INIZIALIZZAZIONE ATTIVATA" | tee -a "$LOG_FILE"
    INIT_PARAM="?initialize=true"
fi

echo "[$(date)] 🚀 Avvio Ingestione Lombardia su $INGEST_ENDPOINT..." | tee -a "$LOG_FILE"

# --- EXECUTION ---
RESPONSE_DATA=$(curl -s -X POST "${INGEST_ENDPOINT}${INIT_PARAM}")
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date)] ✅ Successo! Risposta del Server:" | tee -a "$LOG_FILE"
    echo "$RESPONSE_DATA" | tee -a "$LOG_FILE"
else
    echo "[$(date)] ❌ ERRORE: Impossibile connettersi al backend su porta $API_PORT." | tee -a "$LOG_FILE"
    exit 1
fi

echo "[$(date)] 🏁 Processo terminato." | tee -a "$LOG_FILE"