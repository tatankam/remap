import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import unicodedata

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_text(text: Optional[str]) -> str:
    """Normalize text matching ingest_service.py exactly."""
    if not text:
        return ""
    text = str(text).strip()
    text = unicodedata.normalize("NFKC", text)
    return text

def parse_iso_datetime(date_str: str, default_time: str = "00:00:00") -> Optional[str]:
    """Parse date/time to strict ISO 8601 format for Qdrant (NO Z suffix)."""
    if not date_str:
        return None
    
    date_str = date_str.strip()
    if not date_str:
        return None
    
    try:
        # Handles full ISO formats or just date strings
        if 'T' in date_str or ' ' in date_str:
            # Replace common variations to ensure standard ISO
            clean_str = date_str.replace('Z', '').replace(' ', 'T')
            dt = datetime.fromisoformat(clean_str)
        else:
            dt = datetime.fromisoformat(f"{date_str}T{default_time}")
        return dt.isoformat() 
    except ValueError:
        logger.warning(f"Invalid datetime format: '{date_str}'")
        return None

def parse_ticketsqueeze_csv(csv_path: Path) -> pd.DataFrame:
    """Read a TicketSqueeze CSV file and return as DataFrame."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # dtype=str prevents IDs from being turned into floats (e.g. 123.0)
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    return df

def map_ticketsqueeze_to_event(row: Dict[str, str], delta_type: str) -> Dict[str, Any]:
    """
    Transform a TicketSqueeze row to match expected schema.
    Defensive mapping: looks for 'new_', 'old_', or raw column names.
    """
    
    def get_val(keys: List[str]) -> str:
        """Helper to try multiple possible column names and prefixes."""
        for k in keys:
            # Check prefixes first (for Delta CSV compatibility)
            for prefix in ["new_", "old_", ""]:
                full_key = f"{prefix}{k}"
                if full_key in row and row[full_key]:
                    return normalize_text(row[full_key])
        return ""

    # 1. CORE IDENTITY
    # Matches the key used in csv_delta_service.py
    event_id = get_val(["event_id", "id", "Event ID"])
    title = get_val(["title", "name", "event_name", "Event Name"])
    category = get_val(["category", "category_name", "Category Name"])
    
    # 2. DESCRIPTION & CITY
    description = get_val(["description", "event_description"]) or title
    city = get_val(["city", "venue_city", "venue_city_name", "City Name"])
    
    # 3. LOCATION OBJECT
    venue = get_val(["venue", "venue_name", "Venue Name"])
    venue_addr = get_val(["address", "venue_address", "street_address", "Address"])
    
    # Construct address string for search/display
    full_address = f"{venue_addr}, {city}".strip(", ")

    # COORDINATES (Parsed as floats)
    lat, lon = None, None
    try:
        lat_val = get_val(["latitude", "lat", "geolocation_latitude", "Latitude"])
        lon_val = get_val(["longitude", "lon", "geolocation_longitude", "Longitude"])
        if lat_val: 
            lat = float(lat_val)
        if lon_val: 
            lon = float(lon_val)
    except (ValueError, TypeError):
        pass

    # 4. DATE MANAGEMENT
    # TS usually provides 'Date' and 'Time' columns
    start_date_raw = get_val(["start_date", "event_date", "date", "Date"])
    start_time_raw = get_val(["start_time", "event_time", "time", "Time"])
    
    end_date_raw = get_val(["end_date", "event_date", "date", "Date"])
    end_time_raw = get_val(["end_time"])

    start_date_iso = None
    if start_date_raw:
        if start_time_raw and ":" in start_time_raw:
            start_date_iso = parse_iso_datetime(f"{start_date_raw} {start_time_raw}")
        else:
            start_date_iso = parse_iso_datetime(start_date_raw)
    
    end_date_iso = None
    if end_date_raw:
        if end_time_raw and ":" in end_time_raw:
            end_date_iso = parse_iso_datetime(f"{end_date_raw} {end_time_raw}")
        else:
            # Default end time to end of day if only date is present
            end_date_iso = parse_iso_datetime(end_date_raw, default_time="23:59:59")
    
    url = get_val(["url", "event_url", "ticket_url", "Ticket URL"])

    return {
        "id": event_id,
        "title": title,
        "category": category,
        "description": description,
        "city": city,
        "location": {
            "venue": venue or None,
            "address": full_address or None,
            "lat": lat,
            "lon": lon
        },
        "start_date": start_date_iso,
        "end_date": end_date_iso,
        "url": url,
        "credits": "TicketSqueeze - Events Data",
        "delta_type": delta_type
    }

async def transform_ticketsqueeze_delta_to_json(
    csv_path: Path,
    include_removed: bool = True,
    include_changed: bool = True
) -> List[Dict[str, Any]]:
    """Processes delta.csv into structured events list for Ingest Service."""
    df = parse_ticketsqueeze_csv(csv_path)
    events = []

    if df.empty:
        logger.warning("Delta CSV is empty.")
        return []

    # Iterate once through the dataframe
    for _, row in df.iterrows():
        dtype = row.get("delta_type", "added")
        
        # Filter based on user preference
        if dtype == "removed" and not include_removed: continue
        if dtype == "changed" and not include_changed: continue

        row_dict = row.to_dict()
        event = map_ticketsqueeze_to_event(row_dict, dtype)
        
        # Validates that we at least have an ID and Title before adding
        if event["id"]:
            events.append(event)
        else:
            logger.debug(f"Skipping row missing ID. Raw data: {row_dict}")

    logger.info(f"✅ Transformed {len(events)} events (Mode: {dtype})")
    return events

def save_events_to_json(events: List[Dict[str, Any]], output_path: Path) -> None:
    """Saves the event list to the JSON format expected by the system."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"events": events}, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(events)} events to {output_path}")

async def process_ticketsqueeze_daily_delta(
    delta_csv_path: Path,
    output_json_path: Optional[Path] = None,
    include_removed: bool = True,
    include_changed: bool = True
) -> Dict[str, Any]:
    """Main service entry point."""
    events = await transform_ticketsqueeze_delta_to_json(
        delta_csv_path,
        include_removed=include_removed,
        include_changed=include_changed
    )
    
    if output_json_path:
        save_events_to_json(events, output_json_path)
    
    summary = {
        "total_events": len(events),
        "timestamp": datetime.now().isoformat(),
        "status": "success"
    }
    
    return {"events": events, "summary": summary}