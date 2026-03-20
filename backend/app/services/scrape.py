import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import asyncio
from typing import List, Optional, Tuple, Dict, Any
import json

# Importazione configurazioni centralizzate
from app.core.config import UNPLI_SESSION_ID, UNPLI_API_BASE_URL, UNPLI_WEB_BASE_URL

def clean_html(raw_html: Optional[str]) -> str:
    """Rimuove i tag HTML e pulisce il testo."""
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

async def fetch_unpli_events(
    session: httpx.AsyncClient,
    page_no: int = 1,
    page_size: int = 5,
    session_id: str = UNPLI_SESSION_ID
) -> Optional[List[Dict[str, Any]]]:
    """Recupera la lista eventi base dall'API UNPLI."""
    url = UNPLI_API_BASE_URL
    params = {
        "filterId": "",
        "fields": (
            "id,name,dbCode,owner,isTopEvent,visibilityLevel,date,hasMoreDates,"
            "onlineBookable,location{place,town,regions,country,coordinate{name,long,lat}},"
            "plainDescriptions(len:50){description,type},descriptions(types:[32,33]){description,type},"
            "dateStartTimes,mainCriteria{id,name,value},criteria{groupId,groupName,items{id,name,value}},"
            "eventGroups{id,name},holidayThemes{id,name,order},images(count:1,sizes:[55]){id,name,extension,"
            "copyright,author,license,urls,resolutionX,resolutionY,description},urlFriendlyName,"
            "startTimeDurations{time,weekDays,duration,},guestCards{id,name,type,hasIcon,iconUrl,webLink}"
        ),
        "sortingFields": "date,-topEvent,time",
        "pageNo": page_no,
        "pageSize": page_size,
        "hashF": 0
    }
    headers = {
        "DW-Source": "desklineweb", 
        "DW-SessionID": session_id,
        "Accept": "application/json, text/plain, */*", 
        "Referer": "https://www.unpliveneto.it/",
        "User-Agent": "Mozilla/5.0"
    }
    try:
        response = await session.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data") or data.get("events")
    except Exception as e:
        print(f"Error fetching events: {e}")
    return None

async def fetch_event_details_dates(
    session: httpx.AsyncClient,
    dbCode: str,
    event_id: str,
    session_id: str = UNPLI_SESSION_ID,
    from_date: Optional[str] = None,
    max_retries: int = 5
) -> List[Tuple[str, str, int]]:
    """Recupera le occorrenze future per eventi multi-data."""
    if from_date is None: from_date = "2020-01-01"
    base_api = UNPLI_API_BASE_URL.rstrip('/')
    url = f"{base_api}/{dbCode}/{event_id}"
    fields_value = f'nextOccurrences(fromDate:"{from_date}",count:100){{items{{date,dayOfWeek,startTime,duration}},hasMoreItems}}'
    params = {"fields": fields_value}
    headers = {
        "DW-Source": "desklineweb", 
        "DW-SessionID": session_id,
        "Accept": "application/json, text/plain, */*", 
        "Referer": "https://www.unpliveneto.it/",
        "User-Agent": "Mozilla/5.0"
    }
    backoff = 1
    for attempt in range(max_retries):
        try:
            response = await session.get(url, headers=headers, params=params)
            if response.status_code == 429:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            response.raise_for_status()
            data = response.json()
            items = data.get("nextOccurrences", {}).get("items", [])
            dates_with_info = []
            for item in items:
                if "date" in item:
                    d = item['date'][:10]
                    t = item.get('startTime', '00:00')
                    dates_with_info.append((d, t, item.get("duration", 0)))
            await asyncio.sleep(0.5)
            return dates_with_info
        except:
            await asyncio.sleep(backoff)
    return []

async def transform_events_for_json(events: List[Dict], session_id: str = UNPLI_SESSION_ID) -> List[Dict]:
    """Trasforma i dati nel formato originale piatto aggiungendo il supporto immagini con protocollo HTTPS."""
    transformed = []
    async with httpx.AsyncClient() as session:
        for event in events:
            # Estrazione campi base
            descriptions = event.get("descriptions") or []
            long_description = clean_html(descriptions[0].get("description", "")) if descriptions else ""
            location = event.get("location") or {}
            coordinate = location.get("coordinate") or {}
            venue, city = location.get("place", ""), location.get("town", "")
            title = event.get("name", "")
            criteria = event.get("criteria") or []
            category = criteria[0].get("groupName", "") if criteria and criteria[0] else ""
            db_code, event_id, url_friendly = event.get("dbCode", ""), event.get("id", ""), event.get("urlFriendlyName", "")
            event_url = f"{UNPLI_WEB_BASE_URL.rstrip('/')}/{db_code}/{event_id}/{url_friendly}" if db_code else UNPLI_WEB_BASE_URL
            
            # --- LOGICA IMMAGINI CON FIX HTTPS ---
            image_url = None
            images = event.get("images", [])
            if images:
                image_urls = images[0].get("urls", [])
                if image_urls and len(image_urls) > 0:
                    raw_url = image_urls[0]
                    # Se l'URL inizia con //, aggiungiamo https:
                    if raw_url.startswith("//"):
                        image_url = f"https:{raw_url}"
                    else:
                        image_url = raw_url
            # -------------------------------------

            # Gestione occorrenze date
            raw_occurrences = []
            if event.get("hasMoreDates", False):
                raw_occurrences = await fetch_event_details_dates(session, db_code, event_id, session_id, event.get("date")[:10])
            
            if not raw_occurrences:
                full_date_str = event.get("date", "")
                d_part = full_date_str[:10]
                t_part = full_date_str.split("T")[1][:5] if "T" in full_date_str else "00:00"
                raw_occurrences = [(d_part, t_part, 0)]

            for date_part, time_part, duration_hours in raw_occurrences:
                if time_part in ["00:00", "00:00:00"]:
                    current_localtime = ""
                    final_start_date = date_part
                else:
                    current_localtime = time_part[:5]
                    final_start_date = f"{date_part}T{current_localtime}:00"

                try:
                    dt_start = datetime.strptime(f"{date_part} {time_part[:5]}", "%Y-%m-%d %H:%M")
                    if duration_hours > 0:
                        dt_end = dt_start + timedelta(hours=duration_hours)
                        if dt_end.date() > dt_start.date():
                            dt_end = dt_start.replace(hour=23, minute=59, second=59)
                    else:
                        dt_end = dt_start.replace(hour=23, minute=59, second=59)
                    end_date = dt_end.isoformat()
                except:
                    end_date = f"{date_part}T23:59:59"

                transformed.append({
                    "id": f"UN_{event_id}",
                    "title": title,
                    "category": category,
                    "description": long_description,
                    "city": city,
                    "location": {
                        "venue": venue, 
                        "address": f"{venue}, {city}" if venue and city else venue or city,
                        "lat": coordinate.get("lat"), 
                        "lon": coordinate.get("long")
                    },
                    "start_date": final_start_date,
                    "start_localtime": current_localtime,
                    "end_date": end_date,
                    "url": event_url,
                    "credits": "Dms Veneto, il Destination Management System di Regione del Veneto",
                    "image_url": image_url
                })
    
    return transformed


    """Trasforma i dati nel formato originale piatto aggiungendo il supporto immagini."""
    transformed = []
    async with httpx.AsyncClient() as session:
        for event in events:
            # Estrazione campi base
            descriptions = event.get("descriptions") or []
            long_description = clean_html(descriptions[0].get("description", "")) if descriptions else ""
            location = event.get("location") or {}
            coordinate = location.get("coordinate") or {}
            venue, city = location.get("place", ""), location.get("town", "")
            title = event.get("name", "")
            criteria = event.get("criteria") or []
            category = criteria[0].get("groupName", "") if criteria and criteria[0] else ""
            db_code, event_id, url_friendly = event.get("dbCode", ""), event.get("id", ""), event.get("urlFriendlyName", "")
            event_url = f"{UNPLI_WEB_BASE_URL.rstrip('/')}/{db_code}/{event_id}/{url_friendly}" if db_code else UNPLI_WEB_BASE_URL
            
            # --- NUOVA LOGICA IMMAGINI ---
            image_url = None
            images = event.get("images", [])
            if images:
                image_urls = images[0].get("urls", [])
                if image_urls and len(image_urls) > 0:
                    image_url = image_urls[0] 
            # -----------------------------

            # Gestione occorrenze date
            raw_occurrences = []
            if event.get("hasMoreDates", False):
                raw_occurrences = await fetch_event_details_dates(session, db_code, event_id, session_id, event.get("date")[:10])
            
            if not raw_occurrences:
                full_date_str = event.get("date", "")
                d_part = full_date_str[:10]
                t_part = full_date_str.split("T")[1][:5] if "T" in full_date_str else "00:00"
                raw_occurrences = [(d_part, t_part, 0)]

            for date_part, time_part, duration_hours in raw_occurrences:
                if time_part in ["00:00", "00:00:00"]:
                    current_localtime = ""
                    final_start_date = date_part
                else:
                    current_localtime = time_part[:5]
                    final_start_date = f"{date_part}T{current_localtime}:00"

                try:
                    dt_start = datetime.strptime(f"{date_part} {time_part[:5]}", "%Y-%m-%d %H:%M")
                    if duration_hours > 0:
                        dt_end = dt_start + timedelta(hours=duration_hours)
                        if dt_end.date() > dt_start.date():
                            dt_end = dt_start.replace(hour=23, minute=59, second=59)
                    else:
                        dt_end = dt_start.replace(hour=23, minute=59, second=59)
                    end_date = dt_end.isoformat()
                except:
                    end_date = f"{date_part}T23:59:59"

                # RITORNO ALLA STRUTTURA PIATTA (Originale)
                transformed.append({
                    "id": f"UN_{event_id}",
                    "title": title,
                    "category": category,
                    "description": long_description,
                    "city": city,
                    "location": {
                        "venue": venue, 
                        "address": f"{venue}, {city}" if venue and city else venue or city,
                        "lat": coordinate.get("lat"), 
                        "lon": coordinate.get("long")
                    },
                    "start_date": final_start_date,
                    "start_localtime": current_localtime,
                    "end_date": end_date,
                    "url": event_url,
                    "credits": "Dms Veneto, il Destination Management System di Regione del Veneto",
                    "image_url": image_url  # Aggiunto qui
                })
    
    return transformed