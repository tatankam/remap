import os
import json
import asyncio
import hashlib
import logging
import uuid
from uuid import UUID
from typing import Optional, Dict, Any, List
import unicodedata
import httpx
from fastembed import TextEmbedding, SparseTextEmbedding
from qdrant_client import QdrantClient, models
from app.core.config import QDRANT_SERVER, QDRANT_API_KEY, DENSE_MODEL_NAME, SPARSE_MODEL_NAME, COLLECTION_NAME
from tqdm import tqdm

# Configurazione Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not QDRANT_SERVER or not QDRANT_API_KEY:
    raise EnvironmentError("QDRANT_SERVER or QDRANT_API_KEY non definiti")

# Ottimizzazione RAM
dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME, threads=1)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME, threads=1)
client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=300)

DENSE_VECTOR_NAME = "dense_vector"
SPARSE_VECTOR_NAME = "sparse_vector"

def normalize_text(text):
    if not text: return ""
    return unicodedata.normalize("NFKC", text.strip()[:1000])

def calculate_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def sanitize_id(raw_id: Any) -> str:
    """Genera un UUID v5 DETERMINISTICO basato sulla stringa dell'ID evento."""
    if not raw_id:
        return str(uuid.uuid4())
    str_id = str(raw_id).strip()
    NAMESPACE = uuid.NAMESPACE_DNS
    return str(uuid.uuid5(NAMESPACE, str_id))

def ensure_collection_exists():
    example_text = "Dimension check"
    dense_emb = list(dense_embedding_model.passage_embed([example_text]))[0]
    dense_dim = len(dense_emb)
    
    if not client.collection_exists(COLLECTION_NAME):
        logger.info(f"🚀 Creazione collezione {COLLECTION_NAME}")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={
                DENSE_VECTOR_NAME: models.VectorParams(size=dense_dim, distance=models.Distance.COSINE, on_disk=True),
            },
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: models.SparseVectorParams(index=models.SparseIndexParams(on_disk=True)),
            },
            quantization_config=models.ScalarQuantization(
                scalar=models.ScalarQuantizationConfig(type=models.ScalarType.INT8, always_ram=True)
            )
        )
    
    for field, schema in {"id": "keyword", "location": "geo", "start_date": "datetime"}.items():
        try:
            client.create_payload_index(COLLECTION_NAME, field_name=field, field_schema=schema)
        except: pass

async def async_geocode_structured(venue: str, city: str) -> Optional[Dict[str, float]]:
    base_url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "remap_ingest/1.0"}
    params = {"street": venue, "city": city, "format": "json", "limit": 1}
    async with httpx.AsyncClient() as h_client:
        try:
            resp = await h_client.get(base_url, params=params, headers=headers, timeout=10)
            data = resp.json()
            if data: return {"lat": float(data[0]["lat"]), "lon": float(data[0]["lon"])}
        except Exception as e:
            logger.error(f"❌ Errore Nominatim: {e}")
    return None

async def delete_events_from_qdrant(event_ids: List[str]) -> int:
    """Rimuove fisicamente i punti da Qdrant usando UUID deterministici."""
    if not event_ids: return 0
    point_ids = [sanitize_id(eid) for eid in event_ids]
    try:
        client.delete(
            collection_name=COLLECTION_NAME,
            points_selector=models.PointIdsList(points=point_ids)
        )
        logger.info(f"🗑️ Rimossi {len(point_ids)} punti da Qdrant")
        return len(point_ids)
    except Exception as e:
        logger.error(f"❌ Errore cancellazione: {e}")
        return 0

async def ingest_events_into_qdrant(events: List[Dict[str, Any]], batch_size: int = 32):
    if not events: return {"inserted": 0, "updated": 0, "skipped_unchanged": 0, "deleted": 0}

    # --- GESTIONE CANCELLAZIONI ---
    to_remove_ids = [e.get("id") or e.get("event_id") for e in events if e.get("delta_type") == "removed"]
    deleted_count = 0
    if to_remove_ids:
        deleted_count = await delete_events_from_qdrant(to_remove_ids)
    
    # Filtriamo solo gli eventi da inserire o aggiornare
    active_events = [e for e in events if e.get("delta_type") in ["added", "changed", None]]
    if not active_events:
        return {"inserted": 0, "updated": 0, "skipped_unchanged": 0, "deleted": deleted_count}

    # 1. Geocodifica asincrona (solo per eventi attivi)
    sem = asyncio.Semaphore(1) 
    async def geocode_task(ev):
        loc = ev.get("location", {})
        lat = loc.get("lat") or loc.get("latitude")
        lon = loc.get("lon") or loc.get("longitude")
        if not lat or not lon:
            v, c = loc.get("venue", ""), ev.get("city", "")
            if v and c:
                async with sem:
                    coords = await async_geocode_structured(v, c)
                    if coords:
                        ev["location"].update({"lat": coords["lat"], "lon": coords["lon"]})
                    await asyncio.sleep(1.5)

    await asyncio.gather(*(geocode_task(e) for e in active_events))
    ensure_collection_exists()
    
    inserted = updated = skipped = 0
    total = len(active_events)

    for start in tqdm(range(0, total, batch_size), desc="Ingesting to Qdrant"):
        batch = active_events[start : start + batch_size]
        batch_point_ids = [sanitize_id(e.get("id") or e.get("event_id")) for e in batch]
        batch_texts = [normalize_text(f"{e.get('title','')} {e.get('category','')}") for e in batch]
        local_hashes = [calculate_hash(t) for t in batch_texts]

        existing_points = client.retrieve(collection_name=COLLECTION_NAME, ids=batch_point_ids, with_payload=True)
        existing_map = {str(p.id): p for p in existing_points}

        points_to_upsert = []
        to_embed_indices = []

        for i, event in enumerate(batch):
            pid = batch_point_ids[i]
            existing_p = existing_map.get(pid)
            if existing_p and existing_p.payload.get("hash") == local_hashes[i]:
                skipped += 1
                continue
            to_embed_indices.append(i)

        if not to_embed_indices: continue

        subset_texts = [batch_texts[i] for i in to_embed_indices]
        dense_embs = list(dense_embedding_model.passage_embed(subset_texts))
        sparse_embs = list(sparse_embedding_model.passage_embed(subset_texts))

        for idx, i in enumerate(to_embed_indices):
            event = batch[i]
            pid = batch_point_ids[i]
            
            if pid in existing_map: updated += 1
            else: inserted += 1

            loc = event.get("location", {})
            try:
                lat = float(loc.get("latitude") or loc.get("lat"))
                lon = float(loc.get("longitude") or loc.get("lon"))
                loc_geo = {"lat": lat, "lon": lon}
            except:
                loc_geo = {}

            payload = {**event, "location": {**loc, **loc_geo}, "hash": local_hashes[i]}

            points_to_upsert.append(models.PointStruct(
                id=pid,
                vector={
                    DENSE_VECTOR_NAME: dense_embs[idx].tolist(),
                    SPARSE_VECTOR_NAME: models.SparseVector(
                        indices=list(sparse_embs[idx].indices),
                        values=[float(v) for v in sparse_embs[idx].values]
                    ),
                },
                payload=payload,
            ))

        if points_to_upsert:
            client.upsert(collection_name=COLLECTION_NAME, points=points_to_upsert)

    return {"inserted": inserted, "updated": updated, "skipped_unchanged": skipped, "deleted": deleted_count}