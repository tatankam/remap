#!/bin/bash

# Navigate to the project directory
cd /home/ubuntu/remap || exit 1

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "[$(date)] ❌ ERROR: .env file not found. Aborting."
    exit 1
fi

LOG_FILE="/home/ubuntu/remap/cron.log"

echo "--------------------------------------------------" > "$LOG_FILE"
echo "[$(date)] 🚀 STARTING GLOBAL INGESTION SEQUENCE" >> "$LOG_FILE"
echo "--------------------------------------------------" >> "$LOG_FILE"

# 1. UNPLI
#echo "[$(date)] 📡 Starting UNPLI..." >> "$LOG_FILE"
#./ingestunpli.sh >> "$LOG_FILE" 2>&1

# 2. Lombardia (LO)
echo "[$(date)] 📡 Starting Lombardia..." >> "$LOG_FILE"
./ingestlomb.sh >> "$LOG_FILE" 2>&1

# 3. Ticketmaster IT
echo "[$(date)] 📡 Starting Ticketmaster IT..." >> "$LOG_FILE"
./ingestticketmaster.sh IT >> "$LOG_FILE" 2>&1

# 4. Ticketmaster CA
echo "[$(date)] 📡 Starting Ticketmaster CA..." >> "$LOG_FILE"
./ingestticketmaster.sh CA >> "$LOG_FILE" 2>&1

# 5. Ticketmaster US
echo "[$(date)] 📡 Starting Ticketmaster US..." >> "$LOG_FILE"
./ingestticketmaster.sh US >> "$LOG_FILE" 2>&1

# 6. Feratel
echo "[$(date)] 📡 Starting Feratel..." >> "$LOG_FILE"
./ingestferat.sh >> "$LOG_FILE" 2>&1

echo "--------------------------------------------------" >> "$LOG_FILE"
echo "[$(date)] ✅ GLOBAL INGESTION SEQUENCE COMPLETE" >> "$LOG_FILE"
echo "--------------------------------------------------" >> "$LOG_FILE"