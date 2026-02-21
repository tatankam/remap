#!/bin/bash

# ==============================================================================
# 🎯 TicketSqueeze FULL Pipeline (Docker Host Version)
# Supporta: --initialize per caricamento completo forzato
# Gestisce: Inserimenti, Aggiornamenti e Cancellazioni (Delete)
# ==============================================================================

set -euo pipefail

# 0. GESTIONE PARAMETRI
# ------------------------------------------------------------------------------
INITIALIZE=false
if [[ "${1:-}" == "--initialize" || "${1:-}" == "-initialize" ]]; then
  INITIALIZE=true
fi

# 1. SETUP AMBIENTE E CARICAMENTO .ENV
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f .env ]; then
  # Esporta le variabili ignorando i commenti
  export $(grep -v '^#' .env | xargs)
  echo "✅ Variabili caricate da .env"
else
  echo "❌ Errore: File .env non trovato in $SCRIPT_DIR!" >&2
  exit 1
fi

# Verifica variabili obbligatorie
: ${FTP_HOST:?"Errore: FTP_HOST non impostato"}
: ${FTP_USER:?"Errore: FTP_USER non impostato"}
: ${FTP_PASS:?"Errore: FTP_PASS non impostato"}
: ${FTP_PORT:?"Errore: FTP_PORT non impostato"}
: ${FTP_FILE:?"Errore: FTP_FILE non impostato"}

# Configurazione Percorsi
DATASET_DIR="$SCRIPT_DIR/dataset"
BASE_NAME=$(basename "$FTP_FILE" .csv)
TODAY_DATE=$(date +%Y%m%d)
TODAY_FILE="$DATASET_DIR/${BASE_NAME}_${TODAY_DATE}.csv"
JSON_FILENAME="ts_delta_delta.json"
EMPTY_TEMPLATE="$DATASET_DIR/empty_template.csv"

mkdir -p "$DATASET_DIR"

echo "========================================"
if [ "$INITIALIZE" = true ]; then
    echo " 🚀 MODO INIZIALIZZAZIONE (Full Load)"
else
    echo " 🚀 MODO DELTA (Incremental Load)"
fi
echo "========================================"

# 2. PULIZIA FILE VECCHI
# ------------------------------------------------------------------------------
echo "🧹 Pulizia file obsoleti in corso..."
find "$DATASET_DIR" -type f \( -name "${BASE_NAME}_*.csv" -o -name "ts_delta_*.json" \) -mtime +2 -delete

# 3. DOWNLOAD DA FTP
# ------------------------------------------------------------------------------
echo "📥 Download in corso: $FTP_FILE..."
TMP_PART="$TODAY_FILE.part"

if curl -u "$FTP_USER:$FTP_PASS" \
     --connect-timeout 30 \
     --max-time 600 \
     --fail \
     --silent \
     --show-error \
     "ftp://$FTP_HOST:$FTP_PORT/$FTP_FILE" \
     -o "$TMP_PART"; then

  if [ -s "$TMP_PART" ]; then
    mv "$TMP_PART" "$TODAY_FILE"
    echo "✓ Download completato: $(basename "$TODAY_FILE")"
  else
    echo "❌ Errore: Il file scaricato è vuoto!" >&2
    rm -f "$TMP_PART"
    exit 1
  fi
else
  echo "❌ Errore: Download FTP fallito!" >&2
  rm -f "$TMP_PART"
  exit 1
fi

# 4. LOGICA DELTA / INITIALIZE (CREAZIONE DELTA_TYPE)
# ------------------------------------------------------------------------------
head -n 1 "$TODAY_FILE" > "$EMPTY_TEMPLATE"

OLD_FILE=""
if [ "$INITIALIZE" = true ]; then
    echo "⚠️ Modalità Initialize: confronto con sorgente vuota..."
    OLD_FILE="$EMPTY_TEMPLATE"
else
    LATEST_FILES=($(ls -t "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null | grep -v "$TODAY_DATE" || true))
    
    if [ "${#LATEST_FILES[@]}" -lt 1 ]; then
      echo "⚠️ Nessun file precedente trovato. Uso modalità Full Load automatica..."
      OLD_FILE="$EMPTY_TEMPLATE"
    else
      OLD_FILE="${LATEST_FILES[0]}"
      echo "⚡ Calcolo Delta incremental contro: $(basename "$OLD_FILE")"
    fi
fi

echo "🔄 Generazione Delta CSV..."
curl -s -X POST "http://127.0.0.1:8000/compute-delta" \
     -F "old_file=@$OLD_FILE" \
     -F "new_file=@$TODAY_FILE" > /dev/null

rm -f "$EMPTY_TEMPLATE"

# 5. ELABORAZIONE JSON E INGESTIONE
# ------------------------------------------------------------------------------
DELTA_CSV="$DATASET_DIR/delta.csv"

if [ -f "$DELTA_CSV" ] && [ $(wc -l < "$DELTA_CSV") -gt 1 ]; then
    echo "🔄 Trasformazione Delta CSV -> JSON (Includendo rimossi)..."
    
    # ✅ FIX: Updated URL and correctly passing Boolean flags as Query Parameters
    curl -s -X POST "http://127.0.0.1:8000/ticketsqueeze/process-delta?include_removed=true&include_changed=true" \
         -F "file=@$DELTA_CSV" > /dev/null

    echo "⏳ Attesa sincronizzazione volume (3s)..."
    sleep 3

    if [ -f "$DATASET_DIR/$JSON_FILENAME" ]; then
      echo "🚀 Ingestione JSON in Qdrant..."
      
      # Verifica conteggi per debug
      JSON_COUNT=$(jq '.events | length' "$DATASET_DIR/$JSON_FILENAME")
      CSV_COUNT=$(($(wc -l < "$DELTA_CSV") - 1))
      echo "📊 Records in CSV: $CSV_COUNT | Events in JSON: $JSON_COUNT"

      INGEST_OUTPUT=$(curl -s -X POST "http://127.0.0.1:8000/ingestticketsqueezedelta" \
        -F "file=@$DATASET_DIR/$JSON_FILENAME")
      
      if echo "$INGEST_OUTPUT" | grep -q '"inserted"'; then
        INSERTED=$(echo "$INGEST_OUTPUT" | grep -o '"inserted":[^,}]*' | cut -d: -f2 | tr -d ' ')
        UPDATED=$(echo "$INGEST_OUTPUT" | grep -o '"updated":[^,}]*' | cut -d: -f2 | tr -d ' ')
        SKIPPED=$(echo "$INGEST_OUTPUT" | grep -o '"skipped_unchanged":[^,}]*' | cut -d: -f2 | tr -d ' ')
        DELETED=$(echo "$INGEST_OUTPUT" | grep -o '"deleted":[^,}]*' | cut -d: -f2 | tr -d ' ' || echo "0")
        
        echo "----------------------------------------"
        echo "🎉 SUCCESSFUL INGESTION"
        echo "➕ New Events:    $INSERTED"
        echo "🔄 Updated:       $UPDATED"
        echo "⏭️ Unchanged:     $SKIPPED"
        echo "🗑️ Deleted:       $DELETED"
        echo "----------------------------------------"
      else
        echo "❌ ERRORE: Ingestione fallita."
        echo "Dettaglio risposta: $INGEST_OUTPUT"
        exit 1
      fi
    else
      echo "❌ ERRORE: Il backend non ha prodotto il file $JSON_FILENAME"
      exit 1
    fi
else
    echo "ℹ️ Nessuna modifica rilevata. Fine pipeline."
    [ -f "$DELTA_CSV" ] && rm -f "$DELTA_CSV"
fi

# 7. INFO FINALI
# ------------------------------------------------------------------------------
echo "📊 Final Collection Info:"
curl -s http://127.0.0.1:8000/collection_info
echo -e "\n✅ Pipeline Terminata correttamente: $(date)\n"