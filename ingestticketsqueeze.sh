#!/bin/bash

# ==============================================================================
# 🎯 TicketSqueeze FULL Pipeline (Docker Host Version)
# Sincronizza i dati tra FTP, Host e Backend Docker su porta 8001
# ==============================================================================

set -euo pipefail

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
JSON_FILENAME="ts_delta_delta.json" # Generato dal backend da delta.csv

mkdir -p "$DATASET_DIR"

echo "========================================"
echo " 🚀 Avvio Pipeline: Host -> Backend (:8001)"
echo "========================================"

# 2. PULIZIA FILE VECCHI
# ------------------------------------------------------------------------------
# Rimuove CSV e JSON più vecchi di 2 giorni per risparmiare spazio su disco
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

# 4. CALCOLO DEL DELTA (CSV)
# ------------------------------------------------------------------------------
# Identifica i due file più recenti per confrontarli
LATEST_TWO=($(ls -t "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null | head -2 || true))

if [ "${#LATEST_TWO[@]}" -lt 2 ]; then
  echo "⚠️ Sono necessari almeno 2 file CSV per il delta. Pipeline sospesa."
  exit 0
fi

NEW_FILE="${LATEST_TWO[0]}"
OLD_FILE="${LATEST_TWO[1]}"

echo "⚡ Calcolo Delta tra $(basename "$OLD_FILE") e $(basename "$NEW_FILE")"

# Chiamata al backend per generare delta.csv
curl -s -X POST "http://127.0.0.1:8001/compute-delta" \
     -F "old_file=@$OLD_FILE" \
     -F "new_file=@$NEW_FILE" > /dev/null

# 5. ELABORAZIONE JSON
# ------------------------------------------------------------------------------
DELTA_CSV="$DATASET_DIR/delta.csv"

# Verifichiamo se il delta contiene dati (non è solo l'header)
if [ -f "$DELTA_CSV" ] && [ $(wc -l < "$DELTA_CSV") -gt 1 ]; then
    echo "🔄 Trasformazione Delta CSV -> JSON..."
    
    # Invia delta.csv al container per la trasformazione
    curl -s -X POST "http://127.0.0.1:8001/processticketsqueezedelta" \
         -F "file=@$DELTA_CSV" \
         -F "include_removed=true" \
         -F "include_changed=true" > /dev/null

    # Pulizia immediata del CSV temporaneo
    rm -f "$DELTA_CSV"

    # 6. INGESTIONE FINALE IN QDRANT
    # --------------------------------------------------------------------------
    # Attendiamo la sincronizzazione del volume Docker
    echo "⏳ Attesa sincronizzazione volume (3s)..."
    sleep 3

    if [ -f "$DATASET_DIR/$JSON_FILENAME" ]; then
      echo "🚀 Ingestione JSON in Qdrant tramite :8001..."
      
      # Invia il JSON generato. La sanitizzazione ID avviene nel service.
      INGEST_RESPONSE=$(curl -s -w "HTTP:%{http_code}\n" -X POST "http://127.0.0.1:8001/ingestticketsqueezedelta" \
        -F "file=@$DATASET_DIR/$JSON_FILENAME")
      
      HTTP_CODE=$(echo "$INGEST_RESPONSE" | grep -o 'HTTP:[0-9]*' | cut -d: -f2)
      
      if [ "$HTTP_CODE" == "200" ]; then
        echo "🎉 ECCELLENTE: Ingestione completata con successo!"
        # Pulizia finale per mantenere il sistema snello
        rm -f "$DATASET_DIR/$JSON_FILENAME"
      else
        echo "❌ ERRORE: Ingestione fallita (Codice HTTP $HTTP_CODE)"
        exit 1
      fi
    else
      echo "❌ ERRORE: Il backend non ha prodotto il file $JSON_FILENAME"
      exit 1
    fi
else
    echo "ℹ️ Nessuna modifica rilevata (Delta vuoto). Fine pipeline."
    [ -f "$DELTA_CSV" ] && rm -f "$DELTA_CSV"
fi

echo "========================================"
echo "🎊 PIPELINE TERMINATA CORRETTAMENTE"
echo "========================================"