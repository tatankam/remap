import json
import logging
import urllib.parse
import os
from pathlib import Path
from typing import List, Dict, Any

# Importiamo la configurazione centralizzata dal tuo config.py
from app.core.config import IMPACT_BASE_URL, IMPACT_MEMBER_ID, TM_PROVIDER_PREFIX

logger = logging.getLogger(__name__)

# Utilizziamo il prefisso dal config (che lo legge dal .env)
TM_PREFIX = TM_PROVIDER_PREFIX or "TM"

def transform_tm_event(tm_event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Trasforma un singolo evento dal feed Ticketmaster al formato standard ReMap.
    Gestisce l'affiliazione in modo dinamico per evitare ridondanze.
    """
    venue = tm_event.get("venue", {})
    
    # --- 1. GESTIONE ID CON PREFISSO ---
    raw_id = str(tm_event.get("eventId"))
    prefixed_id = f"{TM_PREFIX}_{raw_id}"
    
    # --- 2. LOGICA AFFILIAZIONE INTELLIGENTE ---
    original_url = tm_event.get("primaryEventUrl", "")
    affiliate_url = original_url
    
    if original_url:
        # Se l'ID membro (IMPACT_MEMBER_ID dal .env) non è già presente nell'URL, 
        # applichiamo il wrapper di affiliazione.
        if IMPACT_MEMBER_ID and IMPACT_MEMBER_ID not in original_url:
            encoded_url = urllib.parse.quote(original_url, safe='')
            affiliate_url = f"{IMPACT_BASE_URL}{encoded_url}"
        else:
            # Se l'ID è già presente o la configurazione manca, usiamo l'URL così com'è
            affiliate_url = original_url

    # --- 3. ESTRAZIONE DATE ---
    start_dt = tm_event.get("eventStartDateTime")
    end_dt = tm_event.get("eventEndDateTime") or start_dt

    # --- 4. MAPPATURA SCHEMA RE-MAP ---
    return {
        "id": prefixed_id, 
        "title": tm_event.get("eventName", "Evento Ticketmaster"),
        "category": tm_event.get("classificationSegment", "Musica/Spettacolo"),
        "description": tm_event.get("eventInfo") or tm_event.get("eventNotes") or "",
        "city": venue.get("venueCity", "N/A"),
        "location": {
            "venue": venue.get("venueName", ""),
            "address": venue.get("venueStreet", ""),
            "lat": float(venue.get("venueLatitude", 0.0)) if venue.get("venueLatitude") else 0.0,
            "lon": float(venue.get("venueLongitude", 0.0)) if venue.get("venueLongitude") else 0.0
        },
        "start_date": start_dt,
        "end_date": end_dt,
        "url": affiliate_url,
        "credits": "Ticketmaster",
        "image_url": tm_event.get("eventImageUrl")
    }

def load_and_transform_tm_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    Carica il dump JSON originale e restituisce la lista di eventi 
    trasformati e pronti per l'ingestione.
    """
    if not file_path.exists():
        logger.error(f"❌ File non trovato: {file_path}")
        return []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # Gestione formati lista o dict con chiave 'events'
            if isinstance(data, list):
                events_list = data
            elif isinstance(data, dict) and "events" in data:
                events_list = data["events"]
            else:
                logger.warning(f"⚠️ Formato JSON inatteso in {file_path.name}")
                events_list = []
                
            return [transform_tm_event(e) for e in events_list]
            
    except Exception as e:
        logger.error(f"❌ Errore durante la trasformazione del file {file_path}: {e}")
        return []