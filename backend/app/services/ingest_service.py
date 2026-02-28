import os
import json
import asyncio
import logging
import uuid
import sqlite3
import hashlib
import time
import unicodedata
import httpx
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
from fastembed import TextEmbedding, SparseTextEmbedding
from qdrant_client import QdrantClient, models
from app.core.config import QDRANT_SERVER, QDRANT_API_KEY, DENSE_MODEL_NAME, SPARSE_MODEL_NAME, COLLECTION_NAME
from tqdm import tqdm
import os

# --- FIX: Redirect all library storage to the writable volume ---
if os.path.exists("/app/dataset"):
    # Fix for FastEmbed / HuggingFace
    os.environ["HF_HOME"] = "/app/dataset/hf_cache"
    os.environ["FASTEMBED_CACHE_PATH"] = "/app/dataset/fastembed_cache"
    
    # Fix for CrewAI / ChromaDB / SQLite (The /.local error)
    os.environ["HOME"] = "/app/dataset"
    os.environ["XDG_DATA_HOME"] = "/app/dataset/.local/share"
    os.environ["XDG_CACHE_HOME"] = "/app/dataset/.cache"
    os.environ["XDG_CONFIG_HOME"] = "/app/dataset/.config"
# --------------------------------------------------------------

# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- PATH CONFIGURATION ---
# Uvicorn starts in /remap/backend. 
# Match the reliable detection used in openroutse_service.py
if os.path.exists("/app"):
    DATASET_DIR = Path("/app") / "dataset"
else:
    # Fallback for local development outside Docker
    DATASET_DIR = Path(__file__).resolve().parent.parent.parent / "dataset"

INGEST_CACHE_DB = DATASET_DIR / "ingest_cache.db"

def init_cache_db():
    """Initializes the SQLite cache in the mounted volume."""
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    
    # Connection with performance optimization (similar to your routing service)
    conn = sqlite3.connect(str(INGEST_CACHE_DB))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS nominatim_cache (
                geo_hash TEXT PRIMARY KEY,
                venue TEXT, address TEXT, city TEXT,
                lat REAL, lon REAL,
                expires INTEGER,
                created INTEGER DEFAULT (strftime('%s','now'))
            )
        """)
        conn.commit()
        logger.info(f"📍 INGEST CACHE DB PATH: {INGEST_CACHE_DB.absolute()}")
    finally:
        conn.close()


init_cache_db()

# --- MODEL & CLIENT INIT ---
dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME, threads=1)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME, threads=1)
client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=300)

DENSE_VECTOR_NAME = "dense_vector"
SPARSE_VECTOR_NAME = "sparse_vector"

def normalize_text(text: str) -> str:
    if not text: return ""
    return unicodedata.normalize("NFKC", str(text).strip()[:1000])

# In ingest_service.py (verifica che sia così)
def sanitize_id(event: Dict) -> str:
    """
    Crea un UUID deterministico basato su ID originale e Data di Inizio.
    Evita collisioni se lo stesso ID evento ha più date.
    """
    raw_id = event.get("id") or event.get("event_id")
    date_str = str(event.get("start_date", "no-date"))
    
    if not raw_id:
        return str(uuid.uuid4())
    
    # La combinazione garantisce unicità per occorrenza
    unique_string = f"{str(raw_id).strip()}_{date_str.strip()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string))

def generate_content_hash(event: Dict) -> str:
    """Generates a hash to detect if the event content itself has changed."""
    content = f"{event.get('title','')}{event.get('description','')}{event.get('start_date','')}"
    return hashlib.sha256(content.encode()).hexdigest()

async def async_geocode_structured(venue: str, city: str, street: str = "") -> Tuple[Optional[Dict[str, float]], bool]:
    """
    Geocodes an address with fallback and caching.
    Returns (coords, is_cache_hit).
    Does NOT cache 0.0 results to allow retries in future runs.
    """
    search_query = street if street else venue
    if not search_query or not city:
        return None, False
    
    geo_key = f"{search_query.lower()}|{city.lower()}"
    geo_hash = hashlib.md5(geo_key.encode()).hexdigest()
    now = int(time.time())

    # 1. READ FROM CACHE
    conn = sqlite3.connect(str(INGEST_CACHE_DB))
    res = conn.execute("SELECT lat, lon FROM nominatim_cache WHERE geo_hash=?", (geo_hash,)).fetchone()
    conn.close()
    
    if res:
        # We only return the cached result if it's not a dummy 0.0 
        # (Though with the new logic, 0.0s won't be in the DB anymore)
        if res[0] != 0.0 and res[1] != 0.0:
            return {"lat": res[0], "lon": res[1]}, True

    # 2. CALL NOMINATIM (WITH DELAY)
    async with httpx.AsyncClient() as h_client:
        try:
            await asyncio.sleep(1.2) # Strict compliance with Nominatim 1s rule
            
            # Try 1: Venue/Street + City
            resp = await h_client.get(
                "https://nominatim.openstreetmap.org/search", 
                params={"street": search_query, "city": city, "format": "json", "limit": 1}, 
                headers={"User-Agent": "remap_ingest_bot/1.6"}, 
                timeout=15
            )
            data = resp.json()
            
            # Try 2: Fallback to City Center only if Try 1 failed
            if not data:
                resp = await h_client.get(
                    "https://nominatim.openstreetmap.org/search", 
                    params={"city": city, "format": "json", "limit": 1}, 
                    headers={"User-Agent": "remap_ingest_bot/1.6"}
                )
                data = resp.json()

            # Result handling
            lat, lon = 0.0, 0.0
            if data:
                lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
            else:
                logger.warning(f"⚠️ Nominatim found nothing for: {search_query}, {city}. Skip caching to allow retry.")

            # 3. WRITE TO CACHE (Only if we found valid coordinates)
            if lat != 0.0 and lon != 0.0:
                conn = sqlite3.connect(str(INGEST_CACHE_DB))
                conn.execute("""
                    INSERT OR REPLACE INTO nominatim_cache (geo_hash, venue, address, city, lat, lon, expires)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (geo_hash, venue, street, city, lat, lon, now + 15552000)) # 180 days
                conn.commit()
                conn.close()
                logger.info(f"✅ Cached new coordinates for: {search_query}, {city}")
            
            return {"lat": lat, "lon": lon}, False

        except Exception as e:
            logger.error(f"❌ Geocoding/Cache Error for {search_query}: {e}")
            
    return None, False




async def ingest_events_into_qdrant(events: List[Dict[str, Any]], batch_size: int = 25):
    """Main pipeline for geocoding and upserting events into Qdrant."""
    if not events:
        return {"inserted": 0, "updated": 0, "deleted": 0}

    # Ensure Collection Exists
    if not client.collection_exists(COLLECTION_NAME):
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={DENSE_VECTOR_NAME: models.VectorParams(size=384, distance=models.Distance.COSINE)},
            sparse_vectors_config={SPARSE_VECTOR_NAME: models.SparseVectorParams(index=models.SparseIndexParams(on_disk=True))}
        )

    to_delete_ids = []
    active_events = []
    
    for e in events:
        q_id = sanitize_id(e)
        if e.get("delta_type") == "removed":
            to_delete_ids.append(q_id)
        else:
            # Normalize location fields
            loc = e.get("location", {})
            e["location"]["lat"] = loc.get("lat") or loc.get("latitude") or 0.0
            e["location"]["lon"] = loc.get("lon") or loc.get("longitude") or 0.0
            e["hash"] = generate_content_hash(e)
            active_events.append((q_id, e))

    # Remove deleted points
    if to_delete_ids:
        client.delete(collection_name=COLLECTION_NAME, points_selector=models.PointIdsList(points=to_delete_ids))

    # STEP 1: GEOCODING (Sequential to respect rate limits)
    if active_events:
        logger.info(f"🌍 Resolving geolocations for {len(active_events)} events...")
        for qid, ev in tqdm(active_events, desc="Geocoding"):
            loc = ev.get("location", {})
            # Only geocode if we don't have coordinates or they are 0.0
            if loc.get("lat") == 0.0:
                venue = loc.get("venue", "")
                city = ev.get("city", "")
                addr = loc.get("address", "")
                street = addr.split(",")[0] if addr else ""
                
                coords, _ = await async_geocode_structured(venue, city, street)
                if coords:
                    ev["location"].update(coords)

    # STEP 2: EMBEDDING & UPSERT (Batched)
    inserted = updated = 0
    total_to_upsert = len(active_events)
    
    for start in tqdm(range(0, total_to_upsert, batch_size), desc="Qdrant Upsert"):
        batch = active_events[start : start + batch_size]
        batch_texts = [normalize_text(f"{ev.get('title','')} {ev.get('description','')} {ev.get('city','')}") for _, ev in batch]
        
        dense_embs = list(dense_embedding_model.passage_embed(batch_texts))
        sparse_embs = list(sparse_embedding_model.passage_embed(batch_texts))

        points = []
        for idx, (q_id, event) in enumerate(batch):
            if event.get("delta_type") == "added":
                inserted += 1
            else:
                updated += 1

            points.append(models.PointStruct(
                id=q_id,
                vector={
                    DENSE_VECTOR_NAME: dense_embs[idx].tolist(),
                    SPARSE_VECTOR_NAME: models.SparseVector(
                        indices=list(sparse_embs[idx].indices),
                        values=[float(v) for v in sparse_embs[idx].values]
                    ),
                },
                payload=event,
            ))
        
        client.upsert(collection_name=COLLECTION_NAME, points=points)

    return {"inserted": inserted, "updated": updated, "deleted": len(to_delete_ids)}