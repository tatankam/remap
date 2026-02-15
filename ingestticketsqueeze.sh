#!/bin/bash

set -euo pipefail

# Load variables from .env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
  echo "✅ Loaded .env"
else
  echo "Error: .env file not found!" >&2
  exit 1
fi

# Check required variables
: ${FTP_HOST:?"FTP_HOST not set"}
: ${FTP_USER:?"FTP_USER not set"}
: ${FTP_PASS:?"FTP_PASS not set"}
: ${FTP_PORT:?"FTP_PORT not set"}
: ${FTP_FILE:?"FTP_FILE not set"}

# Path Setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASET_DIR="$SCRIPT_DIR/dataset"
BASE_NAME=$(basename "$FTP_FILE" .csv)
TODAY_DATE=$(date +%Y%m%d)
TODAY_FILE="$DATASET_DIR/${BASE_NAME}_${TODAY_DATE}.csv"

# Validate dataset directory
mkdir -p "$DATASET_DIR"
if [ ! -w "$DATASET_DIR" ]; then
  echo "Error: dataset folder not writable at $DATASET_DIR!" >&2
  exit 1
fi

echo "========================================"
echo " 🎯 TicketSqueeze FULL Pipeline (Final Perfected)"
echo " 🚀 Target: http://127.0.0.1:8001"
echo "========================================"

# 1. Aggressive Cleanup (Minimize Disk I/O Wait)
echo "🧹 Cleaning up old files..."
find "$DATASET_DIR" -type f \( -name "${BASE_NAME}_*.csv" -o -name "*.json" \) -mtime +2 -delete

# 2. Download
FTP_URL="ftp://$FTP_HOST:$FTP_PORT/$FTP_FILE"
TMP_FILE="$TODAY_FILE.part"

echo "📥 Downloading $FTP_FILE..."
if timeout 600 curl -u "$FTP_USER:$FTP_PASS" \
     --connect-timeout 30 \
     --max-time 600 \
     --retry 3 \
     --retry-delay 5 \
     --retry-connrefused \
     --fail \
     --silent \
     --show-error \
     "$FTP_URL" \
     -o "$TMP_FILE"; then

  if [ -f "$TMP_FILE" ] && [ -s "$TMP_FILE" ]; then
    mv "$TMP_FILE" "$TODAY_FILE"
    echo "✓ Downloaded $(basename "$TODAY_FILE")"
  else
    echo "Error: Empty download!" >&2
    rm -f "$TMP_FILE"
    exit 1
  fi
else
  echo "Error: Download failed!" >&2
  rm -f "$TMP_FILE"
  exit 1
fi

# 3. Delta Computation
LATEST_TWO=($(ls -t "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null | head -2 || true))

if [ "${#LATEST_TWO[@]}" -ge 2 ]; then
  NEW_FILE="${LATEST_TWO[0]}"
  OLD_FILE="${LATEST_TWO[1]}"
  
  echo "⏳ Resting 15s (Clearing RAM/Swap after download)..."
  sleep 15

  echo "⚡ Computing delta: $(basename "$OLD_FILE") → $(basename "$NEW_FILE")"
  DELTA_RESPONSE=$(curl -s -w "HTTP:%{http_code}\n" -X POST "http://127.0.0.1:8001/compute-delta" \
    -F "old_file=@$OLD_FILE" \
    -F "new_file=@$NEW_FILE")
  
  DELTA_HTTP=$(echo "$DELTA_RESPONSE" | grep -o 'HTTP:[0-9]*' | cut -d: -f2 | tr -d ' ' | head -1)
  
  if [ "$DELTA_HTTP" != "200" ]; then
    echo "❌ Delta failed (HTTP $DELTA_HTTP)" >&2
    exit 1
  fi
  
  DELTA_CSV="$DATASET_DIR/delta.csv"
  if [ -f "$DELTA_CSV" ] && [ -s "$DELTA_CSV" ]; then
    
    # 4. JSON Processing (Crucial RAM Isolation)
    echo "⏳ Resting 20s (Cooling down CPU & Disk)..."
    sleep 20

    echo "🔄 Processing delta.csv → JSON..."
    PROCESS_RESPONSE=$(curl -s -w "HTTP:%{http_code}\n" -X POST "http://127.0.0.1:8001/processticketsqueezedelta" \
      -F "file=@$DELTA_CSV" \
      -F "include_removed=true" \
      -F "include_changed=true" \
      -F "clean_id=true")
    
    PROCESS_HTTP=$(echo "$PROCESS_RESPONSE" | grep -o 'HTTP:[0-9]*' | cut -d: -f2 | tr -d ' ' | head -1)
    
    if [ "$PROCESS_HTTP" != "200" ]; then
      echo "❌ Process failed (HTTP $PROCESS_HTTP)" >&2
      exit 1
    fi
    
    # Delete delta immediately to free up I/O for the upcoming Qdrant write
    rm -f "$DELTA_CSV"
    
    JSON_HOST_PATH="ticketsqueeze_delta_delta.json"
    
    # 5. Qdrant Ingestion
    if [ -f "$DATASET_DIR/$JSON_HOST_PATH" ]; then
      echo "⏳ Final Rest 20s (Ensuring Port 8000 is snappy for users)..."
      sleep 20

      echo "🚀 Ingesting to Qdrant..."
      INGEST_RESPONSE=$(curl -s -w "HTTP:%{http_code}\n" -X POST "http://127.0.0.1:8001/ingestticketsqueezedelta" \
        -F "file=@$DATASET_DIR/$JSON_HOST_PATH")
      
      INGEST_HTTP=$(echo "$INGEST_RESPONSE" | grep -o 'HTTP:[0-9]*' | cut -d: -f2 | tr -d ' ' | head -1)
      
      if [ "$INGEST_HTTP" == "200" ]; then
        echo "🎉 COMPLETE PIPELINE SUCCESS!"
        # Final cleanup to keep the host healthy
        rm -f "$DATASET_DIR/$JSON_HOST_PATH"
      else
        echo "❌ Ingest failed (HTTP $INGEST_HTTP)" >&2
        exit 1
      fi
    fi
  else
    echo "ℹ️ No changes found in delta."
  fi
else
  echo "⚠️ Need 2 files for delta - skipping."
fi

echo "========================================"
echo "🎊 PIPELINE FINISHED"
echo "========================================"