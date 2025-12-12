#!/bin/bash

set -euo pipefail

# Load variables from .env
if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
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
TODAY_FILE="$DATASET_DIR/${BASE_NAME}_$(date +%Y%m%d).csv"
YESTERDAY_FILE="$DATASET_DIR/${BASE_NAME}_$(date -d 'yesterday' +%Y%m%d).csv"

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

# CLEANUP: Delete CSV files older than 2 days (keeps today + yesterday)
echo "Cleaning up files older than 2 days..."
DELETED_COUNT=0
while read -r file; do
  rm -f "$file"
  DELETED_COUNT=$((DELETED_COUNT + 1))
  echo "  Deleted: $(basename "$file")"
done < <(find "$DATASET_DIR" -type f -name "${BASE_NAME}_*.csv" -mtime +2 -print)

if [ "$DELETED_COUNT" -eq 0 ]; then
  echo "  No old files to delete"
fi
echo "Cleanup complete ($DELETED_COUNT files deleted)"
echo "----------------------------------------"

# Rotate existing today file to yesterday if it exists
if [ -f "$TODAY_FILE" ]; then
  echo "Rotating existing today file to yesterday..."
  mv "$TODAY_FILE" "$YESTERDAY_FILE"
  echo "✓ $TODAY_FILE → $YESTERDAY_FILE"
fi

# FTP URL
FTP_URL="ftp://$FTP_USER:$FTP_PASS@$FTP_HOST:$FTP_PORT/$FTP_FILE"

echo "Downloading $FTP_FILE from $FTP_HOST:$FTP_PORT..."
echo "Today file: $TODAY_FILE"
echo "Yesterday file: $YESTERDAY_FILE"
echo "Timeout: 600 seconds (10 minutes)"
echo "----------------------------------------"

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
     -o "$TODAY_FILE"; then
  
  # Verify download
  if [ -f "$TODAY_FILE" ] && [ -s "$TODAY_FILE" ]; then
    FILE_SIZE=$(stat -f%z "$TODAY_FILE" 2>/dev/null || stat -c%s "$TODAY_FILE" 2>/dev/null || wc -c < "$TODAY_FILE")
    echo "✓ Successfully downloaded $TODAY_FILE ($FILE_SIZE bytes)"
    
    # Show file status
    if [ -f "$YESTERDAY_FILE" ]; then
      YEST_SIZE=$(stat -f%z "$YESTERDAY_FILE" 2>/dev/null || stat -c%s "$YESTERDAY_FILE" 2>/dev/null || wc -c < "$YESTERDAY_FILE")
      echo "✓ Yesterday file exists: $YEST_SIZE bytes"
    else
      echo "ℹ No yesterday file available"
    fi
  else
    echo "Error: Downloaded file is empty or missing!" >&2
    rm -f "$TODAY_FILE"
    exit 1
  fi
  
else
  echo "Error: Download failed or timed out after 10 minutes!" >&2
  # Revert rotation if download failed
  if [ -f "$YESTERDAY_FILE" ]; then
    mv "$YESTERDAY_FILE" "$TODAY_FILE"
    echo "Reverted: restored previous today file"
  fi
  rm -f "$TODAY_FILE"
  exit 1
fi

echo "Daily update completed successfully!"
echo "Pipeline ready: $TODAY_FILE (new) vs $YESTERDAY_FILE (previous)"
