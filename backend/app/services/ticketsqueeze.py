import pandas as pd
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
import unicodedata



logger = logging.getLogger(__name__)



def normalize_text(text: Optional[str]) -> str:
    """Normalize text matching ingest_service.py exactly."""
    if not text:
        return ""
    text = str(text).strip()
    text = unicodedata.normalize("NFKC", text)
    return text



def parse_iso_datetime(date_str: str, default_time: str = "00:00:00") -> Optional[str]:
    """Parse date/time to strict ISO 8601 format for Qdrant datetime index (NO Z)."""
    if not date_str:
        return None
    
    date_str = date_str.strip()
    if not date_str:
        return None
    
    try:
        # Try full ISO first (handles dates with/without time)
        if 'T' in date_str or ' ' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            # Date only - add default time
            dt = datetime.fromisoformat(f"{date_str}T{default_time}")
        return dt.isoformat()  # ✅ FIXED: NO "Z" suffix
    except ValueError:
        logger.warning(f"Invalid datetime format: '{date_str}'")
        return None



def parse_ticketsqueeze_csv(csv_path: Path) -> pd.DataFrame:
    """Read a TicketSqueeze CSV file and return as DataFrame."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    return df



def extract_delta_from_csv(csv_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract added, removed, and changed rows from a delta CSV."""
    df = parse_ticketsqueeze_csv(csv_path)
    
    if "delta_type" not in df.columns:
        raise ValueError("CSV must contain 'delta_type' column")
    
    added = df[df["delta_type"] == "added"].copy()
    removed = df[df["delta_type"] == "removed"].copy()
    changed = df[df["delta_type"] == "changed"].copy()
    
    return added, removed, changed



def extract_original_columns(row: pd.Series, prefix: str) -> Dict[str, str]:
    """Extract columns with a given prefix from a row."""
    result = {}
    for col in row.index:
        if col.startswith(prefix):
            key = col[len(prefix):]
            result[key] = row[col]
    return result



def map_ticketsqueeze_to_event(row: Dict[str, str], delta_type: str) -> Dict[str, Any]:
    """Transform a TicketSqueeze row to ingest_service.py compatible format."""
    
    # Map columns using exact same normalization as ingest_service.py
    event_id = normalize_text(row.get("event_id", ""))
    title = normalize_text(
        row.get("title") or 
        row.get("name") or 
        row.get("event_name", "")
    )
    category = normalize_text(row.get("category", ""))
    description = normalize_text(row.get("description", "")) or title
    city = normalize_text(
        row.get("city") or 
        row.get("venue_city", "")
    )
    venue = normalize_text(
        row.get("venue") or 
        row.get("venue_name", "")
    )
    address = normalize_text(
        row.get("address") or 
        row.get("venue_address", "")
    )
    
    # Extract latitude/longitude safely
    latitude = None
    longitude = None
    try:
        lat_val = (row.get("latitude") or 
                   row.get("lat") or 
                   row.get("geolocation_latitude", ""))
        lon_val = (row.get("longitude") or 
                   row.get("lon") or 
                   row.get("geolocation_longitude", ""))
        if lat_val: latitude = float(lat_val)
        if lon_val: longitude = float(lon_val)
    except (ValueError, TypeError):
        pass
    
    # CRITICAL: Use exact same datetime parsing pattern as working ingest_service
    # Get raw values first (no manipulation)
    start_date_raw = normalize_text(
        row.get("start_date") or 
        row.get("event_date") or 
        row.get("new_event_date", "")
    )
    start_time_raw = normalize_text(
        row.get("start_time") or 
        row.get("event_time") or 
        row.get("new_event_time", "")
    )
    
    end_date_raw = normalize_text(
        row.get("end_date") or 
        row.get("event_date") or 
        row.get("new_event_date", "")
    )
    end_time_raw = normalize_text(row.get("end_time", ""))
    
    # Parse with strict ISO validation
    start_date = None
    if start_date_raw:
        if start_time_raw:
            start_date = parse_iso_datetime(f"{start_date_raw} {start_time_raw}")
        else:
            start_date = parse_iso_datetime(start_date_raw)
    
    end_date = None
    if end_date_raw:
        if end_time_raw:
            end_date = parse_iso_datetime(f"{end_date_raw} {end_time_raw}")
        else:
            end_date = parse_iso_datetime(end_date_raw, default_time="23:59:59")
    elif start_date:  # Fallback: end = start + 1 day
        try:
            start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            end_dt = start_dt.replace(hour=23, minute=59, second=59)
            end_date = end_dt.isoformat()  # ✅ FIXED: NO "Z" suffix
        except:
            pass
    
    url = normalize_text(row.get("url") or row.get("event_url", ""))
    
    # Build location matching ingest_service.py exactly
    location = {
        "venue": venue or None,
        "address": address or None,
        "latitude": latitude,
        "longitude": longitude
    }
    
    # Exact schema match for ingest_service.py
    event = {
        "id": event_id,
        "title": title,
        "category": category,
        "description": description,  # Used for embedding in ingest_service
        "city": city,
        "location": location,
        "start_date": start_date,
        "end_date": end_date,
        "url": url,
        "credits": "TicketSqueeze - Events Data",
        "delta_type": delta_type
    }
    
    # Log invalid dates for debugging
    if not start_date:
        logger.warning(f"Event {event_id}: Invalid start_date from '{start_date_raw}' '{start_time_raw}'")
    if not end_date:
        logger.warning(f"Event {event_id}: Invalid end_date from '{end_date_raw}' '{end_time_raw}'")
    
    return event



async def transform_ticketsqueeze_delta_to_json(
    csv_path: Path,
    include_removed: bool = False,
    include_changed: bool = True
) -> List[Dict[str, Any]]:
    """Transform delta CSV to ingest_service.py compatible events."""
    added, removed, changed = extract_delta_from_csv(csv_path)
    events = []
    
    # Process added (new_* columns)
    for _, row in added.iterrows():
        row_dict = extract_original_columns(row, "new_")
        row_dict["event_id"] = row.get("event_id", "")
        event = map_ticketsqueeze_to_event(row_dict, "added")
        if event["id"]:
            events.append(event)
    
    # Process removed (old_* columns)
    if include_removed:
        for _, row in removed.iterrows():
            row_dict = extract_original_columns(row, "old_")
            row_dict["event_id"] = row.get("event_id", "")
            event = map_ticketsqueeze_to_event(row_dict, "removed")
            if event["id"]:
                events.append(event)
    
    # Process changed (new_* preferred, fallback to old_*)
    if include_changed:
        for _, row in changed.iterrows():
            row_dict = extract_original_columns(row, "new_")
            if not row_dict:
                row_dict = extract_original_columns(row, "old_")
            row_dict["event_id"] = row.get("event_id", "")
            event = map_ticketsqueeze_to_event(row_dict, "changed")
            if event["id"]:
                events.append(event)
    
    logger.info(f"Transformed {len(events)} events from TicketSqueeze delta CSV")
    return events



def save_events_to_json(events: List[Dict[str, Any]], output_path: Path) -> None:
    """Save events in exact format expected by ingest_service.py."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"events": events}, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(events)} events to {output_path}")



async def process_ticketsqueeze_daily_delta(
    delta_csv_path: Path,
    output_json_path: Optional[Path] = None,
    include_removed: bool = False,
    include_changed: bool = True
) -> Dict[str, Any]:
    """Main entry point: process delta CSV to ingest_service.py JSON."""
    events = await transform_ticketsqueeze_delta_to_json(
        delta_csv_path,
        include_removed=include_removed,
        include_changed=include_changed
    )
    
    if output_json_path:
        save_events_to_json(events, output_json_path)
    
    summary = {
        "total_events": len(events),
        "delta_csv": str(delta_csv_path),
        "output_json": str(output_json_path) if output_json_path else None,
        "timestamp": datetime.now().isoformat()
    }
    
    return {"events": events, "summary": summary}
