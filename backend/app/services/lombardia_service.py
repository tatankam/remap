import httpx
import logging
from typing import List, Dict, Any
from app.core.config import LOMBARDIA_API_ENDPOINT, LOMBARDIA_PROVIDER_PREFIX, LOMBARDIA_API_LIMIT

logger = logging.getLogger(__name__)

async def fetch_lombardia_raw() -> List[Dict[str, Any]]:
    """Recupera l'intero dataset dall'API SODA2 usando i parametri del .env."""
    
    # Costruiamo l'URL dinamico senza hardcoding
    # Assicura che l'endpoint sia presente nel .env
    if not LOMBARDIA_API_ENDPOINT:
        raise ValueError("LOMBARDIA_API_ENDPOINT non configurato nel file .env")
        
    full_url = f"{LOMBARDIA_API_ENDPOINT}?$limit={LOMBARDIA_API_LIMIT}"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        logger.info(f"📡 Richiesta dati Open Data Lombardia: {full_url}")
        response = await client.get(full_url)
        response.raise_for_status()
        data = response.json()
        return data

def transform_lombardia_data(raw_events: List[Dict]) -> List[Dict]:
    """Standardizza lo schema Lombardia usando il prefisso LO_ dal config."""
    standardized = []
    # Usa il prefisso dal .env (es. LO)
    prefix = LOMBARDIA_PROVIDER_PREFIX or "LO"
    
    for item in raw_events:
        # Estrazione Date
        date_in = item.get("data_in", "")[:10]
        ora_in = item.get("ora_in", "")
        date_fine = item.get("data_fine", "")[:10]
        ora_fine = item.get("ora_fine", "")

        # Logic: No time -> Empty string (Consistent with your other sources)
        if ora_in:
            start_date = f"{date_in}T{ora_in}:00"
            start_localtime = ora_in[:5]
        else:
            start_date = date_in
            start_localtime = ""
        
        # End Date Logic (Same-Day constraint)
        if date_fine and date_fine != date_in:
            t_end = ora_fine if ora_fine else "23:59"
            end_date = f"{date_fine}T{t_end}:00"
        else:
            end_date = f"{date_in}T23:59:59"

        # Location
        venue = f"{item.get('toponimo', '')} {item.get('indirizzo', '')}".strip()
        if item.get("civico"):
            venue += f", {item['civico']}"
        city = item.get("comune", "").title()
        
        standardized.append({
            "id": f"{prefix}_{item.get('id')}", 
            "title": item.get("denom", "Sagra/Fiera").strip(),
            "category": item.get("tipo", "Sagra/Fiera"),
            "description": item.get("descriz") or f"Manifestazione a {city}.",
            "city": city,
            "location": {
                "venue": venue,
                "address": f"{venue}, {city} ({item.get('prov')})",
                "lat": float(item.get("geo_y")) if item.get("geo_y") else 0.0,
                "lon": float(item.get("geo_x")) if item.get("geo_x") else 0.0
            },
            "start_date": start_date,
            "start_localtime": start_localtime,
            "end_date": end_date,
            "url": item.get("url_programma", {}).get("url") or item.get("sito_web"),
            "credits": "Dati Open Data Regione Lombardia"
        })
    return standardized