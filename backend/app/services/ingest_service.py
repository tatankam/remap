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

# --- STORAGE ---
if os.path.exists("/app/dataset"):
    os.environ["HF_HOME"] = "/app/dataset/hf_cache"
    os.environ["FASTEMBED_CACHE_PATH"] = "/app/dataset/fastembed_cache"
    os.environ["HOME"] = "/app/dataset"
    os.environ["XDG_DATA_HOME"] = "/app/dataset/.local/share"
    os.environ["XDG_CACHE_HOME"] = "/app/dataset/.cache"
    os.environ["XDG_CONFIG_HOME"] = "/app/dataset/.config"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if os.path.exists("/app"):
    DATASET_DIR = Path("/app") / "dataset"
else:
    DATASET_DIR = Path(__file__).resolve().parent.parent.parent / "dataset"

INGEST_CACHE_DB = DATASET_DIR / "ingest_cache.db"

def init_cache_db():
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(INGEST_CACHE_DB))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE IF NOT EXISTS nominatim_cache (geo_hash TEXT PRIMARY KEY, venue TEXT, address TEXT, city TEXT, lat REAL, lon REAL, expires INTEGER, created INTEGER DEFAULT (strftime('%s','now')))")
        conn.commit()
    finally:
        conn.close()

init_cache_db()

dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME, threads=1)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME, threads=1)
client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=300)

DENSE_VECTOR_NAME = "dense_vector"
SPARSE_VECTOR_NAME = "sparse_vector"

def normalize_text(text: str) -> str:
    if not text: return ""
    return unicodedata.normalize("NFKC", str(text).strip()[:1000])

def sanitize_id(event: Dict) -> str:
    raw_id = event.get("id") or event.get("event_id")
    date_str = str(event.get("start_date", "no-date"))
    unique_string = f"{str(raw_id).strip()}_{date_str.strip()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string))

async def async_geocode_structured(venue: str, city: str, street: str = "") -> Optional[Dict[str, float]]:
    search_query = street if street else venue
    if not search_query or not city: return None
    
    geo_key = f"{search_query.lower()}|{city.lower()}"
    geo_hash = hashlib.md5(geo_key.encode()).hexdigest()
    
    conn = sqlite3.connect(str(INGEST_CACHE_DB))
    res = conn.execute("SELECT lat, lon FROM nominatim_cache WHERE geo_hash=?", (geo_hash,)).fetchone()
    conn.close()
    
    if res and abs(res[0]) > 0.001:
        return {"lat": res[0], "lon": res[1]}

    # ATTESA RIGOROSA (Nominatim 1 req/sec)
    await asyncio.sleep(1.5) 
    
    async with httpx.AsyncClient() as h_client:
        try:
            headers = {"User-Agent": "remap_ingest_bot_v7/7.0"}
            params = {"street": search_query, "city": city, "format": "json", "limit": 1}
            resp = await h_client.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers, timeout=15)
            
            if resp.status_code == 429:
                logger.warning("⚠️ 429 - Nominatim Rate Limit. Attesa 10s...")
                await asyncio.sleep(10)
                return None

            data = resp.json()
            if data:
                lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
                conn = sqlite3.connect(str(INGEST_CACHE_DB))
                conn.execute("INSERT OR REPLACE INTO nominatim_cache (geo_hash, venue, address, city, lat, lon, expires) VALUES (?,?,?,?,?,?,?)",
                             (geo_hash, venue, street, city, lat, lon, int(time.time()) + 15552000))
                conn.commit()
                conn.close()
                return {"lat": lat, "lon": lon}
        except Exception as e:
            logger.error(f"❌ Geocoding error: {e}")
    return None

async def ingest_events_into_qdrant(events: List[Dict[str, Any]], batch_size: int = 25):
    if not events: return {"inserted": 0, "updated": 0, "deleted": 0}

    if not client.collection_exists(COLLECTION_NAME):
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={DENSE_VECTOR_NAME: models.VectorParams(size=384, distance=models.Distance.COSINE)},
            sparse_vectors_config={SPARSE_VECTOR_NAME: models.SparseVectorParams(index=models.SparseIndexParams(on_disk=True))}
        )

    processed_events = []
    for e in events:
        l_date = e.get("start_localdate") or (str(e["start_date"])[:10] if e.get("start_date") else None)
        l_time = e.get("start_localtime") or e.get("local_time")
        
        # --- CRUCIALE: CONVERSIONE FLOAT ---
        loc = e.get("location", {}).copy()
        try:
            lat = float(loc.get("lat") or 0.0)
            lon = float(loc.get("lon") or 0.0)
        except:
            lat, lon = 0.0, 0.0

        e["location"]["lat"] = lat
        e["location"]["lon"] = lon
        e["start_localtime"] = l_time
        e["start_localdate"] = l_date
        processed_events.append((sanitize_id(e), e))

    logger.info(f"🌍 Analisi di {len(processed_events)} eventi...")
    for qid, ev in tqdm(processed_events, desc="Geocoding"):
        loc = ev.get("location", {})
        
        # SKIP se lat/lon sono già validi (> 0.001)
        if abs(loc.get("lat", 0.0)) > 0.001 and abs(loc.get("lon", 0.0)) > 0.001:
            continue
        
        venue, city = loc.get("venue", ""), ev.get("city", "")
        street = loc.get("address", "").split(",")[0] if loc.get("address") else ""
        
        if (street or venue) and city:
            coords = await async_geocode_structured(venue, city, street)
            if coords:
                ev["location"].update(coords)

    inserted = 0
    for start in tqdm(range(0, len(processed_events), batch_size), desc="Qdrant Upsert"):
        batch = processed_events[start : start + batch_size]
        batch_texts = [normalize_text(f"{ev.get('title','')} {ev.get('description','')} {ev.get('city','')}") for _, ev in batch]
        
        dense_embs = list(dense_embedding_model.passage_embed(batch_texts))
        sparse_embs = list(sparse_embedding_model.passage_embed(batch_texts))

        points = []
        for idx, (q_id, event) in enumerate(batch):
            inserted += 1
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

    return {"inserted": inserted, "updated": 0, "deleted": 0}