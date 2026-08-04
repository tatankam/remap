import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any

def generate_content_hash(event: Dict) -> str:
    """Crea un'impronta digitale del contenuto dell'evento."""
    content = f"{event.get('title','')}{event.get('description','')}{event.get('start_date','')}{event.get('end_date','')}"
    return hashlib.sha256(content.encode()).hexdigest()

def get_event_key(e: Dict) -> str:
    """Genera la chiave univoca (ID + start_date) speculare a quella usata in sanitize_id."""
    raw_id = e.get("id") or e.get("event_id") or ""
    date_str = str(e.get("start_date", "no-date"))
    return f"{str(raw_id).strip()}_{date_str.strip()}"

def compute_json_delta(old_file: Path, new_file: Path) -> List[Dict[str, Any]]:
    """Confronta due file JSON usando chiavi composte (ID + start_date)."""
    if not old_file.exists():
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

    # Mappatura tramite chiave composta (id + start_date)
    old_map = {get_event_key(e): e for e in old_events}
    new_map = {get_event_key(e): e for e in new_events}

    delta_results = []

    # 1. Nuovi (added) e Modificati (changed)
    for key, new_ev in new_map.items():
        if key not in old_map:
            new_ev["delta_type"] = "added"
            delta_results.append(new_ev)
        else:
            if generate_content_hash(new_ev) != generate_content_hash(old_map[key]):
                new_ev["delta_type"] = "changed"
                delta_results.append(new_ev)

    # 2. Rimossi (removed) - mantiene i vecchi dati per rigenerare lo stesso UUID
    for key, old_ev in old_map.items():
        if key not in new_map:
            old_ev["delta_type"] = "removed"
            delta_results.append(old_ev)

    return delta_results