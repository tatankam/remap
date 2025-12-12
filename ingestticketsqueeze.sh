#!/bin/bash

set -euo pipefail

# [ALL PREVIOUS CODE UNCHANGED - cleanup, download, delta compute...]

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
  
  # Parse summary for DISPLAY ONLY
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
  
  # ✅ KEY FIX: Process if delta.csv exists
  DELTA_CSV="$DATASET_DIR/delta.csv"
  if [ -f "$DELTA_CSV" ] && [ -s "$DELTA_CSV" ]; then
    echo "⚡ delta.csv exists ($(stat -c%s "$DELTA_CSV" 2>/dev/null || echo "?") bytes) → PROCESSING!"
    
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
    
    # ✅ CRITICAL FIX: HARDCODE JSON FILENAME (backend returns empty path)
    JSON_HOST_PATH="ticketsqueeze_delta_delta.json"
    EVENTS_COUNT=$(echo "$PROCESS_JSON" | jq -r '.summary.events // 0' 2>/dev/null || echo "unknown")
    
    echo "✅ JSON processing complete (events: $EVENTS_COUNT)"
    echo "📄 Expecting: $DATASET_DIR/$JSON_HOST_PATH"
    
    # ⏳ WAIT for JSON file
    echo "⏳ Waiting for $JSON_HOST_PATH..."
    for i in {1..20}; do
      if [ -f "$DATASET_DIR/$JSON_HOST_PATH" ] && [ -s "$DATASET_DIR/$JSON_HOST_PATH" ]; then
        JSON_SIZE=$(stat -c%s "$DATASET_DIR/$JSON_HOST_PATH" 2>/dev/null || echo "?")
        echo "✓ JSON ready ($JSON_SIZE bytes)"
        break
      fi
      echo "  Wait $i/20... (ls: $(ls "$DATASET_DIR"/*.json 2>/dev/null || echo "no json"))"
      sleep 1
    done
    
    if [ ! -f "$DATASET_DIR/$JSON_HOST_PATH" ]; then
      echo "❌ $JSON_HOST_PATH not found! Available files:" >&2
      ls -la "$DATASET_DIR"/*.json 2>/dev/null || echo "  (no JSON files)" >&2
      exit 1
    fi
    
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
    
    echo "🎉 Qdrant ingestion complete!"
    echo "  🗑️  Deleted: $DELETED"
    echo "  ➕ Inserted: $INSERTED"
    echo "  ✏️  Updated: $UPDATED"
    echo "  ⏭️  Skipped: $SKIPPED"
    
  else
    echo "ℹ️ No delta.csv produced - skipping processing"
  fi
else
  echo "⚠️ Less than 2 files available - skipping delta"
fi

echo "========================================"
echo "🎊 COMPLETE PIPELINE SUCCESS!"
echo "📁 Files:"
ls -la "$DATASET_DIR"/*.csv "$DATASET_DIR"/*.json 2>/dev/null || echo "No files"
echo "========================================"
