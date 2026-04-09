import re
import html
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

# Constants to match your standalone script logic
DSI_NS = {"dsi": "http://interface.deskline.net/DSI/XSD"}
NO_NS = {}

def unescape_soap(raw: str) -> str:
    """Robust SOAP unescaping from your script logic."""
    start_tag = "<GetDataResult>"
    end_tag = "</GetDataResult>"
    start = raw.find(start_tag) + len(start_tag)
    end = raw.find(end_tag, start)
    if start == -1 or end == -1:
        return ""
    content = raw[start:end].strip()
    return (content.replace("&lt;", "<")
               .replace("&gt;", ">")
               .replace("&quot;", '"')
               .replace("&amp;", "&"))

def safe_find(elem: ET.Element, path: str) -> ET.Element:
    """Namespace-agnostic find helper."""
    res = elem.find(path, DSI_NS)
    if res is not None:
        return res
    return elem.find(path.replace("dsi:", ""), NO_NS)

def first_text_safe(elem: ET.Element, path: str) -> str:
    """Safe text extraction from your script logic."""
    res = safe_find(elem, path)
    return res.text.strip() if res is not None and res.text else ""

def parse_date_time(date_str: str, time_str: str = "") -> str:
    """ISO format generator."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if time_str:
            h, m = map(int, time_str.split(":")[:2])
            dt = dt.replace(hour=h, minute=m)
        return dt.isoformat()
    except:
        return f"{date_str}T00:00:00"

def parse_feratel_data(events_path: Path, keyvalues_path: Path) -> List[Dict[str, Any]]:
    """Main transformation service integrating feratel_to_events.py logic."""
    
    # 1. Load Facilities/Categories
    facility_map = {}
    try:
        kv_tree = ET.parse(keyvalues_path)
        kv_root = kv_tree.getroot()
        for fac in kv_root.findall(".//Facility", NO_NS):
            fac_id = fac.get("Id")
            name_elem = fac.find("Name", NO_NS)
            if fac_id and name_elem is not None:
                it_trans = name_elem.find("Translation[@Language='it']", NO_NS)
                facility_map[fac_id] = it_trans.text.strip() if it_trans is not None and it_trans.text else "Evento"
    except Exception as e:
        logger.warning(f"⚠️ Error loading facilities: {e}")

    # 2. Process Events
    try:
        raw = events_path.read_text(encoding="utf-8")
        xml_content = unescape_soap(raw)
        if not xml_content:
            return []
        root = ET.fromstring(xml_content)
    except Exception as e:
        logger.error(f"❌ XML Parse Error: {e}")
        return []

    events_list = []
    event_elems = root.findall(".//dsi:Event", DSI_NS) or root.findall(".//Event", NO_NS)

    for event in event_elems:
        try:
            event_id = event.get("Id", "")
            details = safe_find(event, "dsi:Details")
            if details is None: continue

            # Title & Description
            title = first_text_safe(event, "dsi:Details/dsi:Names/dsi:Translation[@Language='it']") or \
                    first_text_safe(event, "dsi:Details/dsi:Names/dsi:Translation") or "Senza Titolo"
            
            description = first_text_safe(event, "dsi:Descriptions/dsi:Description[@Type='EventHeader']") or \
                          first_text_safe(event, "dsi:Descriptions/dsi:Description[@Type='EventHeaderShort']")

            # Location & Coordinates
            lat, lon = 0.0, 0.0
            pos = safe_find(details, "dsi:Position")
            if pos is not None:
                lat, lon = float(pos.get("Latitude", 0)), float(pos.get("Longitude", 0))

            city, venue, street = "", "Sede", ""
            addresses = event.findall("dsi:Addresses/dsi:Address", DSI_NS)
            for addr in addresses:
                if addr.get("Type") == "Venue":
                    city = first_text_safe(addr, "dsi:Town")
                    venue = first_text_safe(addr, "dsi:Company") or "Sede"
                    street = first_text_safe(addr, "dsi:AddressLine1")
                    break

            # Image & URL extraction
            image_url = ""
            docs_node = safe_find(event, "dsi:Documents")
            if docs_node is not None:
                for doc in docs_node.findall("dsi:Document", DSI_NS):
                    if doc.get("Class") == "Image" and doc.get("Type") == "EventHeader":
                        u_node = safe_find(doc, "dsi:URL")
                        if u_node is not None and u_node.text:
                            image_url = u_node.text.strip("[]<>/ ")
                            break

            url = ""
            for addr in addresses:
                u_node = safe_find(addr, "dsi:URL")
                if u_node is not None and u_node.text:
                    url = u_node.text.strip("[]<> ")
                    if url: break

            # Category
            fac_node = safe_find(event, "dsi:Facilities/dsi:Facility")
            cat_id = fac_node.get("Id") if fac_node is not None else ""
            category = facility_map.get(cat_id, "Manifestazione")

            # Dates Processing
            dates_node = safe_find(details, "dsi:Dates")
            if dates_node is not None:
                for d_node in dates_node.findall("dsi:Date", DSI_NS):
                    start_date = d_node.get("From", "")
                    start_time = d_node.get("Time", "00:00")
                    if start_date:
                        events_list.append({
                            "id": f"FRT_{event_id}_{start_date}_{start_time.replace(':', '')}",
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
            logger.warning(f"Skipping event: {e}")
            continue

    return events_list