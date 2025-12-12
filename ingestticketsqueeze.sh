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

# Get script directory and dataset folder (HOST PATH)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASET_DIR="$SCRIPT_DIR/dataset"
BASE_NAME=$(basename "$FTP_FILE" .csv)

# Today / yesterday logical names (by date)
TODAY_DATE=$(date +%Y%m%d)
YESTERDAY_DATE=$(date -d 'yesterday' +%Y%m%d 2>/dev/null || date -v-1d +%Y%m%d)
TODAY_FILE="$DATASET_DIR/${BASE_NAME}_${TODAY_DATE}.csv"
YESTERDAY_FILE="$DATASET_DIR/${BASE_NAME}_${YESTERDAY_DATE}.csv"

# Validate dataset directory
mkdir -p "$DATASET_DIR"
if [ ! -w "$DATASET_DIR" ]; then
  echo "Error: dataset folder not writable at $DATASET_DIR!" >&2
  exit 1
fi

# Check dependencies
command -v curl >/dev/null 2>&1 || { echo "Error: curl required!" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "Error: jq required!" >&2; exit 1; }

echo "========================================"
echo " 🎯 TicketSqueeze FULL Pipeline"
echo " 📁 Dataset dir: $DATASET_DIR"
echo " 📄 Base name  : $BASE_NAME"
echo " 🚀 http://127.0.0.1:8000"
echo "========================================"

# 🧹 CLEANUP: Delete CSV files older than 2 days
echo "🧹 Cleaning up files older than 2 days..."
DELETED_COUNT=0
while read -r file; do
  [ -z "$file" ] && continue
  rm -f "$file"
  DELETED_COUNT=$((DELETED_COUNT + 1))
  echo "  Deleted: $(basename "$file")"
done < <(find "$DATASET_DIR" -type f -name "${BASE_NAME}_*.csv" -mtime +2 -print 2>/dev/null || true)

echo "Cleanup complete ($DELETED_COUNT files deleted)"
echo "----------------------------------------"

# Keep only 2 most recent files
echo "🔄 Ensuring at most two latest CSVs..."
EXISTING=()
while IFS= read -r f; do
  EXISTING+=("$f")
done < <(ls -t "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null || true)

if [ "${#EXISTING[@]}" -gt 2 ]; then
  for ((i=2; i<${#EXISTING[@]}; i++)); do
    echo "  Removing extra: $(basename "${EXISTING[$i]}")"
    rm -f "${EXISTING[$i]}"
  done
fi

echo "Current CSV files:"
ls -1 "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null || echo "  (none yet)"
echo "----------------------------------------"

# 📥 DOWNLOAD
FTP_URL="ftp://$FTP_HOST:$FTP_PORT/$FTP_FILE"
TMP_FILE="$TODAY_FILE.part"

echo "📥 Downloading $FTP_FILE → $TODAY_FILE"
if timeout 600 curl -u "$FTP_USER:$FTP_PASS" \
     --connect-timeout 30 \
     --max-time 600 \
     --retry 3 \
     --retry-delay 5 \
     --retry-connrefused \
     --fail \
     --silent \
     --show-error \
     --progress-bar \
     "$FTP_URL" \
     -o "$TMP_FILE"; then

  if [ -f "$TMP_FILE" ] && [ -s "$TMP_FILE" ]; then
    mv "$TMP_FILE" "$TODAY_FILE"
    FILE_SIZE=$(stat -f%z "$TODAY_FILE" 2>/dev/null || stat -c%s "$TODAY_FILE" 2>/dev/null || wc -c < "$TODAY_FILE")
    echo "✓ Downloaded $TODAY_FILE ($FILE_SIZE bytes)"
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

# Find latest two files for delta
LATEST_TWO=()
while IFS= read -r f; do
  LATEST_TWO+=("$f")
done < <(ls -t "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null | head -2 || true)

if [ "${#LATEST_TWO[@]}" -ge 2 ]; then
  NEW_FILE="${LATEST_TWO[0]}"
  OLD_FILE="${LATEST_TWO[1]}"
  
  echo "⚡ Computing delta: $OLD_FILE → $NEW_FILE"
  
  # STEP 1: Compute delta
  echo "📤 Uploading CSVs to /compute-delta..."
  DELTA_RESPONSE=$(curl -s -w "HTTP:%{http_code}\n" -X POST "http://127.0.0.1:8000/compute-delta" \
    -F "old_file=@$OLD_FILE" \
    -F "new_file=@$NEW_FILE")
  
  DELTA_HTTP=$(echo "$DELTA_RESPONSE" | grep -o 'HTTP:[0-9]*' | cut -d: -f2 | tr -d ' ' | head -1)
  DELTA_JSON=$(echo "$DELTA_RESPONSE" | sed '/HTTP:/d' | sed 's/[[:space:]]*$//')
  
  if [ "$DELTA_HTTP" != "200" ]; then
    echo "❌ Delta failed (HTTP $DELTA_HTTP)" >&2
    exit 1
  fi
  
  # Parse summary for DISPLAY ONLY (ignore buggy numbers)
  if echo "$DELTA_JSON" | jq . >/dev/null 2>&1; then
    ADDED=$(echo "$DELTA_JSON" | jq -r '.summary.added // 0' 2>/dev/null || echo "0")
    REMOVED=$(echo "$DELTA_JSON" | jq -r '.summary.removed // 0' 2>/dev/null || echo "0")
    CHANGED=$(echo "$DELTA_JSON" | jq -r '.summary.changed // 0' 2>/dev/null || echo "0")
    TOTAL=$(echo "$DELTA_JSON" | jq -r '.summary.total // 0' 2>/dev/null || echo "0")
    DELTA_PATH=$(echo "$DELTA_JSON" | jq -r '.csv_path // "unknown"' 2>/dev/null || echo "unknown")
  else
    ADDED="?" REMOVED="?" CHANGED="?" TOTAL="?" DELTA_PATH="unknown"
  fi
  
  echo "📊 Delta summary: $ADDED added, $REMOVED removed, $CHANGED changed ($TOTAL total)"
  echo "💾 Delta.csv saved: $DELTA_PATH"
  
  # ✅ CRITICAL FIX: Process IF delta.csv exists + non-empty (ignores JSON numbers!)
  DELTA_CSV="$DATASET_DIR/delta.csv"
  if [ -f "$DELTA_CSV" ] && [ -s "$DELTA_CSV" ]; then
    echo "⚡ delta.csv exists ($(stat -c%s "$DELTA_CSV" 2>/dev/null || echo "?") bytes) → PROCESSING!"
    
    # ⏳ WAIT for volume sync (if needed)
    echo "⏳ Ensuring delta.csv ready..."
    for i in {1..5}; do
      if [ -s "$DELTA_CSV" ]; then
        break
      fi
      sleep 1
    done
    
    # STEP 2: Process delta.csv → JSON
    echo "🔄 Processing delta.csv → JSON..."
    PROCESS_RESPONSE=$(curl -s -w "HTTP:%{http_code}\n" -X POST "http://127.0.0.1:8000/processticketsqueezedelta" \
      -F "file=@$DELTA_CSV" \
      -F "include_removed=true" \
      -F "include_changed=true")
    
    PROCESS_HTTP=$(echo "$PROCESS_RESPONSE" | grep -o 'HTTP:[0-9]*' | cut -d: -f2 | tr -d ' ' | head -1)
    PROCESS_JSON=$(echo "$PROCESS_RESPONSE" | sed '/HTTP:/d' | sed 's/[[:space:]]*$//')
    
    if [ "$PROCESS_HTTP" != "200" ]; then
      echo "❌ Process failed (HTTP $PROCESS_HTTP)" >&2
      exit 1
    fi
    
    JSON_PATH=$(echo "$PROCESS_JSON" | jq -r '.saved_path // "unknown"' 2>/dev/null || echo "unknown")
    EVENTS_COUNT=$(echo "$PROCESS_JSON" | jq -r '.summary.events // 0' 2>/dev/null || echo "0")
    
    echo "✅ JSON created: $JSON_PATH ($EVENTS_COUNT events)"
    
    # ⏳ WAIT: Sync JSON from container → host
    JSON_HOST_PATH=$(basename "$JSON_PATH")
    echo "⏳ Waiting for JSON on host ($DATASET_DIR/$JSON_HOST_PATH)..."
    for i in {1..15}; do
      if [ -f "$DATASET_DIR/$JSON_HOST_PATH" ] && [ -s "$DATASET_DIR/$JSON_HOST_PATH" ]; then
        echo "✓ JSON ready ($(stat -c%s "$DATASET_DIR/$JSON_HOST_PATH" 2>/dev/null || echo "?") bytes)"
        break
      fi
      sleep 1
    done
    
    if [ -f "$DATASET_DIR/$JSON_HOST_PATH" ]; then
      # STEP 3: Ingest JSON to Qdrant
      echo "🚀 Ingesting $DATASET_DIR/$JSON_HOST_PATH to Qdrant..."
      INGEST_RESPONSE=$(curl -s -w "HTTP:%{http_code}\n" -X POST "http://127.0.0.1:8000/ingestticketsqueezedelta" \
        -F "file=@$DATASET_DIR/$JSON_HOST_PATH")
      
      INGEST_HTTP=$(echo "$INGEST_RESPONSE" | grep -o 'HTTP:[0-9]*' | cut -d: -f2 | tr -d ' ' | head -1)
      INGEST_JSON=$(echo "$INGEST_RESPONSE" | sed '/HTTP:/d' | sed 's/[[:space:]]*$//')
      
      if [ "$INGEST_HTTP" != "200" ]; then
        echo "❌ Ingest failed (HTTP $INGEST_HTTP)" >&2
        exit 1
      fi
      
      DELETED=$(echo "$INGEST_JSON" | jq -r '.deleted // 0' 2>/dev/null || echo "0")
      INSERTED=$(echo "$INGEST_JSON" | jq -r '.inserted // 0' 2>/dev/null || echo "0")
      UPDATED=$(echo "$INGEST_JSON" | jq -r '.updated // 0' 2>/dev/null || echo "0")
      SKIPPED=$(echo "$INGEST_JSON" | jq -r '.skipped_unchanged // 0' 2>/dev/null || echo "0")
      POINTS=$(echo "$INGEST_JSON" | jq -r '.points_count // 0' 2>/dev/null || echo "0")
      
      echo "🎉 Qdrant ingestion complete!"
      echo "  🗑️  Deleted: $DELETED"
      echo "  ➕ Inserted: $INSERTED"
      echo "  ✏️  Updated: $UPDATED"
      echo "  ⏭️  Skipped: $SKIPPED"
      echo "  📊 Total points: $POINTS"
    else
      echo "❌ JSON not synced to host!" >&2
      exit 1
    fi
  else
    echo "ℹ️ No delta.csv produced - skipping processing"
  fi
else
  echo "⚠️ Less than 2 files available - skipping delta (run again tomorrow)"
fi

echo "========================================"
echo "🎊 COMPLETE PIPELINE SUCCESS!"
echo "📁 Files in $DATASET_DIR:"
ls -la "$DATASET_DIR"/*.csv "$DATASET_DIR"/*.json 2>/dev/null || echo "No pipeline files"
echo "🔍 Collection status:"
curl -s "http://127.0.0.1:8000/collection_info" | jq . 2>/dev/null || echo "Service unavailable"
echo "========================================"
