import os
import json
import asyncio
import hashlib
import logging
from uuid import uuid4
from typing import Optional, Dict, Any, List
import unicodedata
import httpx
from fastembed import TextEmbedding, SparseTextEmbedding
from qdrant_client import QdrantClient, models
from app.core.config import QDRANT_SERVER, QDRANT_API_KEY, DENSE_MODEL_NAME, SPARSE_MODEL_NAME, COLLECTION_NAME
from tqdm import tqdm

# Detailed Logging Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not QDRANT_SERVER or not QDRANT_API_KEY:
    raise EnvironmentError("QDRANT_SERVER or QDRANT_API_KEY not defined in .env file")

# ✅ OPTIMIZATION: threads=1 prevents RAM/CPU spikes on 1GB machine
dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME, threads=1)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME, threads=1)
client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=300)

DENSE_VECTOR_NAME = "dense_vector"
SPARSE_VECTOR_NAME = "sparse_vector"

def normalize_text(text):
    if not text:
        return ""
    text = text.strip()[:1000]
    text = unicodedata.normalize("NFKC", text)
    return text

def calculate_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def ensure_collection_exists():
    """Restored full collection setup with INT8 Quantization and logging"""
    example_text = "Test for embedding dimension calculation."
    dense_emb = list(dense_embedding_model.passage_embed([example_text]))[0]
    dense_dim = len(dense_emb)
    
    if not client.collection_exists(COLLECTION_NAME):
        logger.info(f"🚀 Creating collection {COLLECTION_NAME} (Dense: {dense_dim})")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={
                DENSE_VECTOR_NAME: models.VectorParams(
                    size=dense_dim,
                    distance=models.Distance.COSINE,
                    on_disk=True),
            },
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: models.SparseVectorParams(
                    index=models.SparseIndexParams(on_disk=True)
                ),
            },
            quantization_config=models.ScalarQuantization(
                scalar=models.ScalarQuantizationConfig(
                    type=models.ScalarType.INT8,
                    always_ram=True,
                ),
            ),
            hnsw_config=models.HnswConfigDiff(
                m=16,
                ef_construct=100,
                full_scan_threshold=10000
            )
        )
        logger.info("✅ Collection created with INT8 quantization.")
    
    # Payload indices
    for field, schema in {"id": "keyword", "location": "geo", "start_date": "datetime", "end_date": "datetime"}.items():
        try:
            client.create_payload_index(COLLECTION_NAME, field_name=field, field_schema=schema)
        except Exception:
            pass

async def async_geocode_structured(venue: str, city: str, region: str = "Veneto", country: str = "Italy") -> Optional[Dict[str, float]]:
    base_url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "remap_ingest/1.0"}
    params = {"street": venue, "city": city, "state": region, "country": country, "format": "json", "limit": 1}
    async with httpx.AsyncClient() as client_http:
        try:
            response = await client_http.get(base_url, params=params, headers=headers, timeout=10)
            data = response.json()
            if data:
                logger.info(f"📍 Geocoded: {venue}, {city} -> {data[0]['lat']}, {data[0]['lon']}")
                return {"lat": float(data[0]["lat"]), "lon": float(data[0]["lon"])}
        except Exception as e:
            logger.warning(f"⚠️ Geocoding failed for {venue}: {str(e)}")
    return None

async def ingest_events_into_qdrant(events: List[Dict[str, Any]], batch_size: int = 32):
    if not events:
        logger.warning("Empty events list provided to ingestion.")
        return {"inserted": 0, "updated": 0, "skipped_unchanged": 0}

    # 1. Geocoding Phase
    semaphore = asyncio.Semaphore(5)
    async def geocode_event(event):
        loc = event.get("location", {})
        if loc.get("latitude") and loc.get("longitude"):
            return
        venue, city = loc.get("venue", "").strip(), event.get("city", "").strip()
        if venue and city:
            async with semaphore:
                coords = await async_geocode_structured(venue, city)
            if coords:
                event["location"]["latitude"] = coords["lat"]
                event["location"]["longitude"] = coords["lon"]

    logger.info(f"🌍 Starting geocoding for {len(events)} events...")
    await asyncio.gather(*(geocode_event(event) for event in events))

    ensure_collection_exists()
    
    inserted = updated = skipped_unchanged = 0
    total = len(events)
    
    logger.info(f"⚡ Ingesting {total} events (Batch Size: {batch_size})")

    for start in tqdm(range(0, total, batch_size), desc="Qdrant Ingestion"):
        batch = events[start : start + batch_size]
        batch_ids = [str(e.get("id") or e.get("event_id", "")) for e in batch]
        batch_texts = [normalize_text(f"{e.get('title', '')} {e.get('category', '')}") for e in batch]
        local_hashes = [calculate_hash(t) for t in batch_texts]

        # ✅ BULK CHECK (Efficiency Fix)
        existing_points = client.retrieve(
            collection_name=COLLECTION_NAME,
            ids=[eid for eid in batch_ids if eid],
            with_payload=True,
            with_vectors=False
        )
        existing_map = {p.payload.get("id"): p for p in existing_points}

        points_to_upsert = []
        to_embed_indices = []

        for i, eid in enumerate(batch_ids):
            if not eid: continue
            existing_p = existing_map.get(eid)
            if existing_p and existing_p.payload.get("hash") == local_hashes[i]:
                skipped_unchanged += 1
                continue
            to_embed_indices.append(i)

        if not to_embed_indices:
            continue

        # AI Embedding
        subset_texts = [batch_texts[i] for i in to_embed_indices]
        dense_embeddings = list(dense_embedding_model.passage_embed(subset_texts))
        sparse_embeddings = list(sparse_embedding_model.passage_embed(subset_texts))

        for idx, i in enumerate(to_embed_indices):
            event = batch[i]
            existing_p = existing_map.get(batch_ids[i])
            point_id = existing_p.id if existing_p else str(uuid4())
            if existing_p: updated += 1
            else: inserted += 1

            loc = event.get("location", {})
            loc_geo = {"lat": float(loc["latitude"]), "lon": float(loc["longitude"])} if loc.get("latitude") else {}
            
            payload = {**event, "location": {**loc, **loc_geo}, "hash": local_hashes[i]}

            points_to_upsert.append(models.PointStruct(
                id=point_id,
                vector={
                    DENSE_VECTOR_NAME: dense_embeddings[idx].tolist(),
                    SPARSE_VECTOR_NAME: models.SparseVector(
                        indices=list(sparse_embeddings[idx].indices),
                        values=[float(v) for v in sparse_embeddings[idx].values]
                    ),
                },
                payload=payload,
            ))

        if points_to_upsert:
            is_last = (start + batch_size >= total)
            client.upsert(collection_name=COLLECTION_NAME, points=points_to_upsert, wait=is_last)
            logger.info(f"📤 Batch uploaded: {len(points_to_upsert)} points (wait={is_last})")

    # Final stats logging
    collection_info = client.get_collection(COLLECTION_NAME)
    logger.info(f"🎉 Ingestion finished. Total points in collection: {collection_info.points_count}")
    
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped_unchanged": skipped_unchanged,
        "points_count": collection_info.points_count
    }