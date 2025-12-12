#!/bin/bash

set -euo pipefail

# Load variables from .env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
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

# Get script directory and dataset folder
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASET_DIR="$SCRIPT_DIR/dataset"
BASE_NAME=$(basename "$FTP_FILE" .csv)

# Today / yesterday logical names (by date)
TODAY_DATE=$(date +%Y%m%d)
YESTERDAY_DATE=$(date -d 'yesterday' +%Y%m%d 2>/dev/null || date -v-1d +%Y%m%d)
TODAY_FILE="$DATASET_DIR/${BASE_NAME}_${TODAY_DATE}.csv"
YESTERDAY_FILE="$DATASET_DIR/${BASE_NAME}_${YESTERDAY_DATE}.csv"

# Validate dataset directory
if [ ! -d "$DATASET_DIR" ]; then
  echo "Error: dataset folder not found at $DATASET_DIR!" >&2
  exit 1
fi

if [ ! -w "$DATASET_DIR" ]; then
  echo "Error: dataset folder not writable at $DATASET_DIR!" >&2
  exit 1
fi

# Check curl availability
if ! command -v curl >/dev/null 2>&1; then
  echo "Error: curl is required but not installed!" >&2
  exit 1
fi

echo "========================================"
echo " TicketSqueeze daily ingestion + delta"
echo " Dataset dir: $DATASET_DIR"
echo " Base name  : $BASE_NAME"
echo "========================================"

# CLEANUP: Delete CSV files older than 2 days
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

# DOWNLOAD
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
  
  # Call FastAPI delta endpoint
  curl -s -X POST "http://127.0.0.1:8000/compute-delta" \
    -F "old_file=@$OLD_FILE" \
    -F "new_file=@$NEW_FILE" \
    | jq -r '. | "\(.summary.added) added, \(.summary.removed) removed, \(.summary.changed) changed (\(.summary.total) total), saved: \(.csv_path)"'
    
  echo "✓ Delta.csv ready in $DATASET_DIR!"
else
  echo "⚠️ Less than 2 files - skipping delta (run again tomorrow)"
fi

echo "🎉 Pipeline complete! Ready for processticketsqueezedelta → ingestticketsqueezedelta"
