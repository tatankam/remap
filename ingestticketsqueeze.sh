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
echo " TicketSqueeze daily ingestion"
echo " Dataset dir: $DATASET_DIR"
echo " Base name  : $BASE_NAME"
echo " Today      : $TODAY_FILE"
echo " Yesterday  : $YESTERDAY_FILE"
echo "========================================"

# CLEANUP: Delete CSV files older than 2 days (keeps last two by mtime)
echo "Cleaning up files older than 2 days..."
DELETED_COUNT=0
while read -r file; do
  [ -z "$file" ] && continue
  rm -f "$file"
  DELETED_COUNT=$((DELETED_COUNT + 1))
  echo "  Deleted: $(basename "$file")"
done < <(find "$DATASET_DIR" -type f -name "${BASE_NAME}_*.csv" -mtime +2 -print 2>/dev/null || true)

if [ "$DELETED_COUNT" -eq 0 ]; then
  echo "  No old files to delete"
fi
echo "Cleanup complete ($DELETED_COUNT files deleted)"
echo "----------------------------------------"

# ROTATION BY MDATE: keep last two downloads even if there are gaps
echo "Ensuring at most two latest CSVs are present (by mtime)..."
EXISTING=()
while IFS= read -r f; do
  EXISTING+=("$f")
done < <(ls -t "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null || true)

if [ "${#EXISTING[@]}" -gt 2 ]; then
  # Keep the two newest, delete the rest
  for ((i=2; i<${#EXISTING[@]}; i++)); do
    echo "  Removing extra file: $(basename "${EXISTING[$i]}")"
    rm -f "${EXISTING[$i]}"
  done
fi

echo "Current CSV files:"
ls -1 "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null || echo "  (none yet)"
echo "----------------------------------------"

# FTP URL
FTP_URL="ftp://$FTP_HOST:$FTP_PORT/$FTP_FILE"

echo "Downloading $FTP_FILE from $FTP_HOST:$FTP_PORT..."
echo "Target (today): $TODAY_FILE"
echo "Timeout: 600 seconds (10 minutes)"
echo "----------------------------------------"

TMP_FILE="$TODAY_FILE.part"

# Download with timeout, retries, and progress
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

  # Verify download
  if [ -f "$TMP_FILE" ] && [ -s "$TMP_FILE" ]; then
    mv "$TMP_FILE" "$TODAY_FILE"
    FILE_SIZE=$(stat -f%z "$TODAY_FILE" 2>/dev/null || stat -c%s "$TODAY_FILE" 2>/dev/null || wc -c < "$TODAY_FILE")
    echo "✓ Successfully downloaded $TODAY_FILE ($FILE_SIZE bytes)"
  else
    echo "Error: Downloaded file is empty or missing!" >&2
    rm -f "$TMP_FILE"
    exit 1
  fi
else
  echo "Error: Download failed or timed out after 10 minutes!" >&2
  rm -f "$TMP_FILE"
  exit 1
fi

# Re-evaluate the two most recent files for delta calculation
LATEST_TWO=()
while IFS= read -r f; do
  LATEST_TWO+=("$f")
done < <(ls -t "$DATASET_DIR"/${BASE_NAME}_*.csv 2>/dev/null | head -2 || true)

if [ "${#LATEST_TWO[@]}" -ge 2 ]; then
  NEW_FILE="${LATEST_TWO[0]}"
  OLD_FILE="${LATEST_TWO[1]}"
  echo "Delta will be computed between:"
  echo "  OLD: $OLD_FILE"
  echo "  NEW: $NEW_FILE"
else
  echo "Warning: Less than 2 CSV files available; delta step will have to wait for another successful run."
fi

echo "Daily update completed successfully!"
