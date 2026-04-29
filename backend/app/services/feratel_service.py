import re
import html
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def unescape_soap(raw: str) -> str:
    """Estrae il contenuto XML pulito da qualsiasi wrapper SOAP Feratel."""
    match = re.search(r"<(?:Get\w+Result)>(.*?)</(?:Get\w+Result)>", raw, re.DOTALL)
    if not match:
        return raw
    
    content = match.group(1).strip()
    return (content.replace("&lt;", "<")
               .replace("&gt;", ">")
               .replace("&quot;", '"')
               .replace("&amp;", "&"))

def find_agnostic(element: ET.Element, tag_name: str):
    """Trova il PRIMO tag ignorando il namespace."""
    for elem in element.iter():
        if elem.tag.split('}')[-1] == tag_name:
            return elem
    return None

def find_all_agnostic(element: ET.Element, tag_name: str):
    """Trova TUTTI i tag ignorando il namespace."""
    results = []
    for elem in element.iter():
        if elem.tag.split('}')[-1] == tag_name:
            results.append(elem)
    return results

def get_text_agnostic(element: ET.Element, tag_name: str) -> str:
    """Estrae il testo di un tag ignorando il namespace."""
    found = find_agnostic(element, tag_name)
    return found.text.strip() if found is not None and found.text else ""

def parse_date_time(date_str: str, time_str: str = "") -> str:
    """Genera un timestamp ISO 8601."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if time_str:
            h, m = map(int, time_str.split(":")[:2])
            dt = dt.replace(hour=h, minute=m)
        return dt.isoformat()
    except:
        return f"{date_str}T00:00:00"

def parse_feratel_data(events_path: Path, keyvalues_path: Path) -> List[Dict[str, Any]]:
    """Trasforma i dati Feratel con ID univoco per data (ID_STARTDATE)."""
    
    facility_map = {}
    town_map = {}

    # 1. CARICAMENTO MAPPINGS
    try:
        kv_raw = keyvalues_path.read_text(encoding="utf-8")
        kv_content = unescape_soap(kv_raw)
        if kv_content:
            kv_root = ET.fromstring(kv_content)
            for fac in find_all_agnostic(kv_root, "Facility"):
                fac_id = fac.get("Id")
                if not fac_id: continue
                for trans in find_all_agnostic(fac, "Translation"):
                    if trans.get("Language") == "it" and trans.text:
                        facility_map[fac_id] = trans.text.strip()
                        break
            for loc in find_all_agnostic(kv_root, "Location"):
                loc_id = loc.get("Id")
                if not loc_id: continue
                for trans in find_all_agnostic(loc, "Translation"):
                    if trans.get("Language") == "it" and trans.text:
                        town_map[loc_id] = trans.text.strip()
                        break
    except Exception as e:
        logger.error(f"❌ Errore caricamento KeyValues: {e}")

    # 2. PROCESSO EVENTI
    try:
        raw = events_path.read_text(encoding="utf-8")
        xml_content = unescape_soap(raw)
        if not xml_content: return []
        root = ET.fromstring(xml_content)
    except Exception as e:
        logger.error(f"❌ Errore parsing eventi: {e}")
        return []

    events_list = []
    for event in find_all_agnostic(root, "Event"):
        try:
            event_id = event.get("Id", "")
            details = find_agnostic(event, "Details")
            if details is None: continue

            # Titolo
            title = "Senza Titolo"
            names = find_agnostic(details, "Names")
            if names is not None:
                for trans in find_all_agnostic(names, "Translation"):
                    if trans.get("Language") == "it" and trans.text:
                        title = trans.text.strip()
                        break

            # Descrizione
            description = ""
            descriptions_node = find_agnostic(event, "Descriptions")
            if descriptions_node is not None:
                for desc in find_all_agnostic(descriptions_node, "Description"):
                    if desc.get("Type") == "EventHeader" and desc.text:
                        description = desc.text.strip()
                        break

            # Coordinate
            lat, lon = 0.0, 0.0
            pos = find_agnostic(details, "Position")
            if pos is not None:
                try:
                    lat, lon = float(pos.get("Latitude", 0)), float(pos.get("Longitude", 0))
                except: pass

            # Città
            towns_node = find_agnostic(details, "Towns")
            city = ""
            if towns_node is not None:
                item = find_agnostic(towns_node, "Item")
                if item is not None:
                    city = town_map.get(item.get("Id"), "")

            # Categorie
            found_categories = []
            facilities_node = find_agnostic(event, "Facilities")
            if facilities_node is not None:
                for f_node in find_all_agnostic(facilities_node, "Facility"):
                    f_id = f_node.get("Id")
                    if f_id in facility_map:
                        found_categories.append(facility_map[f_id])
            category = ", ".join(dict.fromkeys(found_categories)) if found_categories else "Manifestazione"

            # Venue e Indirizzo
            venue_from_location = ""
            loc_tag = find_agnostic(details, "Location")
            if loc_tag is not None:
                for trans in find_all_agnostic(loc_tag, "Translation"):
                    if trans.get("Language") == "it" and trans.text:
                        venue_from_location = trans.text.strip()
                        break

            addr_venue, street, addr_city_fallback = "", "", ""
            addresses_node = find_agnostic(event, "Addresses")
            if addresses_node is not None:
                for addr in find_all_agnostic(addresses_node, "Address"):
                    if addr.get("Type") == "Venue":
                        addr_city_fallback = get_text_agnostic(addr, "Town")
                        addr_venue = get_text_agnostic(addr, "Company")
                        street = get_text_agnostic(addr, "AddressLine1")
                        break
            if not city: city = addr_city_fallback
            venue = venue_from_location or addr_venue or "Sede"

            # Immagine
            image_url = ""
            docs_node = find_agnostic(event, "Documents")
            if docs_node is not None:
                for doc in find_all_agnostic(docs_node, "Document"):
                    if doc.get("Class") == "Image" and doc.get("Type") == "EventHeader":
                        image_url = get_text_agnostic(doc, "URL").strip("[]<>/ ")
                        break

            # URL
            url = ""
            if addresses_node is not None:
                for addr in find_all_agnostic(addresses_node, "Address"):
                    u = get_text_agnostic(addr, "URL").strip("[]<> ")
                    if u: 
                        url = u
                        break

            # Date
            dates_parent = find_agnostic(details, "Dates")
            all_dates = find_all_agnostic(dates_parent, "Date") if dates_parent is not None else []
            if not all_dates:
                single_date = find_agnostic(details, "Date")
                if single_date is not None: all_dates = [single_date]

            for d_node in all_dates:
                start_date = d_node.get("From", "")
                start_time = d_node.get("Time", "00:00")
                if start_date:
                    # MODIFICA: Appendiamo la data all'ID per rendere ogni punto univoco
                    unique_id = f"FRT_{event_id}_{start_date}"
                    
                    events_list.append({
                        "id": unique_id,
                        "title": title,
                        "category": category,
                        "description": description,
                        "city": city,
                        "location": {
                            "venue": venue,
                            "address": f"{street}, {city}".strip(", "),
                            "lat": lat,
                            "lon": lon
                        },
                        "start_date": parse_date_time(start_date, start_time),
                        "start_localtime": start_time[:5],
                        "end_date": parse_date_time(start_date, "23:59:59"),
                        "url": url,
                        "image_url": image_url,
                        "credits": "Dms Veneto, il Destination Management System di Regione Veneto"
                    })
        except Exception as e:
            logger.warning(f"Salto evento {event.get('Id')}: {e}")
            continue

    return events_list