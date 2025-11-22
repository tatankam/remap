import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import asyncio
from typing import List, Optional, Tuple, Dict, Any


def clean_html(raw_html: Optional[str]) -> str:
    if not raw_html:
        return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


async def fetch_unpli_events(
    session: httpx.AsyncClient,
    page_no: int = 1,
    page_size: int = 5,
    session_id: str = "G1758362087062"
) -> Optional[List[Dict[str, Any]]]:
    url = "https://webapi.deskline.net/unpliveneto/it/events"
    params = {
        "filterId": "",
        "fields": (
            "id,name,dbCode,owner,isTopEvent,visibilityLevel,date,hasMoreDates,"
            "onlineBookable,location{place,town,regions,country,coordinate{name,long,lat}},"
            "plainDescriptions(len:50){description,type},descriptions(types:[32,33]){description,type},"
            "dateStartTimes,mainCriteria{id,name,value},criteria{groupId,groupName,items{id,name,value}},"
            "eventGroups{id,name},holidayThemes{id,name,order},images(count:1,sizes:[54]){id,name,extension,"
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
    except httpx.HTTPStatusError as e:
        print(f"Request failed with status code {e.response.status_code}: {e}")
    except Exception as e:
        print(f"Error fetching unpli events: {e}")
    return None


async def fetch_event_details_dates(
    session: httpx.AsyncClient,
    dbCode: str,
    event_id: str,
    session_id: str,
    from_date: Optional[str] = None,
    max_retries: int = 5
) -> List[Tuple[str, int]]:
    if from_date is None:
        from_date = "2020-01-01"
    url = f"https://webapi.deskline.net/unpliveneto/it/events/{dbCode}/{event_id}"
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
                retry_after = response.headers.get("Retry-After")
                wait_time = int(retry_after) if retry_after and retry_after.isdigit() else backoff
                print(f"Received 429, retrying after {wait_time} seconds (attempt {attempt + 1})...")
                await asyncio.sleep(wait_time)
                backoff = min(backoff * 2, 60)
                continue
            response.raise_for_status()
            data = response.json()
            items = data.get("nextOccurrences", {}).get("items", [])
            dates_with_duration = []
            for item in items:
                if "date" in item:
                    dates_with_duration.append((f"{item['date'][:10]}T{item.get('startTime', '00:00')}:00", item.get("duration", 0)))
            await asyncio.sleep(1)  # polite pause
            return dates_with_duration
        except httpx.RequestError as e:
            print(f"Request error: {e}, attempt {attempt + 1} of {max_retries}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
    print("Max retries reached for event details, skipping dates.")
    return []


async def transform_events_for_json(events: List[Dict], session_id: str) -> List[Dict]:
    transformed = []
    async with httpx.AsyncClient() as session:
        total_events = len(events)
        for i, event in enumerate(events, 1):
            print(f"Processing event {i} of {total_events}: ID {event.get('id', '')}")
            descriptions = event.get("descriptions") or []
            long_description = ""
            if descriptions and isinstance(descriptions, list):
                raw_html = descriptions[0].get("description", "")
                long_description = clean_html(raw_html)

            location = event.get("location") or {}
            coordinate = location.get("coordinate") or {}

            venue = location.get("place", "")
            city = location.get("town", "")
            regions = location.get("regions") or []
            region_str = ", ".join(regions) if regions else ""

            title = event.get("name", "")

            category = ""
            criteria = event.get("criteria") or []
            if criteria and isinstance(criteria, list) and len(criteria) > 0:
                category = criteria[0].get("groupName", "") if criteria[0] else ""

            base_url = "https://www.unpliveneto.it/eventi-delle-pro-loco-in-veneto/#/eventi/"
            db_code = event.get("dbCode", "")
            event_id = event.get("id", "")
            url_friendly = event.get("urlFriendlyName", "")
            event_url = f"{base_url}{db_code}/{event_id}/{url_friendly}" if db_code and event_id and url_friendly else base_url

            dates_with_durations = []
            if event.get("hasMoreDates", False):
                from_date = event.get("date")[:10]
                dates_with_durations = await fetch_event_details_dates(session, dbCode=db_code, event_id=event_id, session_id=session_id, from_date=from_date)
                if not dates_with_durations:
                    dates_with_durations = [(event.get("date"), 0)]
            else:
                dates_with_durations = [(event.get("date"), 0)]

            for start_date, duration_hours in dates_with_durations:
                try:
                    dt_start = datetime.strptime(start_date[:19], "%Y-%m-%dT%H:%M:%S")
                    if duration_hours == 0:
                        dt_end = dt_start.replace(hour=23, minute=59, second=59, microsecond=0)
                    else:
                        dt_end = dt_start + timedelta(hours=duration_hours)
                    end_date = dt_end.isoformat() + "Z"
                except Exception:
                    end_date = start_date

                address = ", ".join([venue, city]) if venue and city else venue or city

                unique_id = f"{event.get('id', '')}_{start_date[:10]}"  # unique event ID per edition by adding date

                transformed.append({
                    "id": unique_id,
                    "title": title,
                    "category": category,
                    "description": long_description,
                    "city": city,
                    "location": {
                        "venue": venue,
                        "address": address,
                        "latitude": coordinate.get("lat"),
                        "longitude": coordinate.get("long")
                    },
                    "start_date": start_date,
                    "end_date": end_date,
                    "url": event_url,
                    "credits": "Dms Veneto, il Destination Management System di Regione del Veneto"
                })
    return transformed
