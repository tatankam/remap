import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any

def generate_content_hash(event: Dict) -> str:
    """
    Creates a fingerprint of the event content. 
    Matches the logic in ingest_service to ensure consistency.
    """
    # We hash the parts that matter. If these change, the event is 'changed'.
    content = f"{event.get('title','')}{event.get('description','')}{event.get('start_date','')}{event.get('end_date','')}"
    return hashlib.sha256(content.encode()).hexdigest()

def compute_json_delta(old_file: Path, new_file: Path) -> List[Dict[str, Any]]:
    """
    Compares two UNPLI JSON files using Master IDs.
    Returns a list of events tagged with 'delta_type': added, changed, or removed.
    """
    if not old_file.exists():
        # If no old file, every event in new_file is 'added'
        with open(new_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            events = data.get("events", [])
            for e in events:
                e["delta_type"] = "added"
            return events

    with open(old_file, 'r', encoding='utf-8') as f:
        old_data = json.load(f)
    with open(new_file, 'r', encoding='utf-8') as f:
        new_data = json.load(f)

    old_events = old_data.get("events", [])
    new_events = new_data.get("events", [])

    # Map by Master ID
    old_map = {e['id']: e for e in old_events}
    new_map = {e['id']: e for e in new_events}

    delta_results = []

    # 1. Detect Added and Changed
    for eid, new_ev in new_map.items():
        if eid not in old_map:
            new_ev["delta_type"] = "added"
            delta_results.append(new_ev)
        else:
            # Content check via hash
            if generate_content_hash(new_ev) != generate_content_hash(old_map[eid]):
                new_ev["delta_type"] = "changed"
                delta_results.append(new_ev)

    # 2. Detect Removed
    for eid, old_ev in old_map.items():
        if eid not in new_map:
            old_ev["delta_type"] = "removed"
            delta_results.append(old_ev)

    return delta_results