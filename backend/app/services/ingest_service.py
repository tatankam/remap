import os
import json
import asyncio
import hashlib
import logging
from uuid import uuid4
from typing import Optional, Dict, Any

import unicodedata
import httpx
from dotenv import load_dotenv
from tqdm import tqdm
from fastembed import TextEmbedding, SparseTextEmbedding
from qdrant_client import QdrantClient, models
from app.core.config import QDRANT_SERVER, QDRANT_API_KEY, DENSE_MODEL_NAME, SPARSE_MODEL_NAME, COLLECTION_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_text(text):
    if not text:
        return ""
    text = text.strip()[:1000]  # FREE TIER: Truncate for memory
    text = unicodedata.normalize("NFKC", text)
    return text

if not QDRANT_SERVER or not QDRANT_API_KEY:
    raise EnvironmentError("QDRANT_SERVER or QDRANT_API_KEY not defined in .env file")

dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME)
client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=30000)  # FREE TIER: 30s timeout

DENSE_VECTOR_NAME = "dense_vector"
SPARSE_VECTOR_NAME = "sparse_vector"

def ensure_collection_exists():
    """FREE TIER OPTIMIZED: INT8 quantization + on-disk vectors"""
    example_text = "Test for embedding dimension calculation."
    dense_emb = list(dense_embedding_model.passage_embed([example_text]))[0]
    dense_dim = len(dense_emb)
    
    if not client.collection_exists(COLLECTION_NAME):
        logger.info(f"🚀 Creating FREE TIER optimized collection {COLLECTION_NAME}")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=dense_dim, 
                distance=models.Distance.COSINE,
                on_disk=True  # ✅ Store vectors on disk (saves RAM)
            ),
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: models.SparseVectorParams(),
            },
            quantization_config=models.ScalarQuantization(  # ✅ 75% RAM savings
                scalar=models.ScalarQuantizationConfig(
                    type=models.ScalarType.INT8,
                    always_ram=True,  # Quantized vectors stay in RAM
                ),
            ),
            hnsw_config=models.HnswConfigDiff(  # ✅ Balanced precision
                m=16,
                ef_construct=100,
                full_scan_threshold=10000
            )
        )
        logger.info("✅ Collection created with INT8 quantization (4x more capacity!)")
    
    # Payload indices
    payload_indices = {
        "id": "keyword",
        "location": "geo",
        "start_date": "datetime",
        "end_date": "datetime"
    }
    for field_name, field_schema in payload_indices.items():
        try:
            client.create_payload_index(
                collection_name=COLLECTION_NAME,
                field_name=field_name,
                field_schema=field_schema,
            )
        except Exception:
            pass  # Already exists

async def async_geocode_structured(
    venue: str, city: str, region: str = "Veneto", country: str = "Italy"
) -> Optional[Dict[str, float]]:
    base_url = "https://nominatim.openstreetmap.org/search"
    headers = {"User-Agent": "convert_to_geo/1.0"}
    params_list = [
        {"street": venue, "city": city, "state": region, "country": country, "format": "json", "limit": 1},
        {"city": city, "state": region, "country": country, "format": "json", "limit": 1},
        {"street": venue, "city": city, "country": country, "format": "json", "limit": 1},
        {"street": venue, "state": region, "country": country, "format": "json", "limit": 1},
    ]
    async with httpx.AsyncClient() as client_http:
        for params in params_list:
            try:
                response = await client_http.get(base_url, params=params, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                if data:
                    return {"lat": float(data[0]["lat"]), "lon": float(data[0]["lon"])}
            except (httpx.HTTPError, ValueError):
                pass
            await asyncio.sleep(1)
    return None

def calculate_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

async def ingest_events_from_file(json_path: str) -> Dict[str, Any]:
    logger.info(f"🚀 FREE TIER OPTIMIZED: Loading events from {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        events_data = json.load(f)

    events = events_data.get("events", [])
    if not events:
        return {"inserted": 0, "updated": 0, "skipped_unchanged": 0, "message": "No events"}
    
    semaphore = asyncio.Semaphore(5)

    def is_valid_lat_lon(lat, lon):
        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            return False
        return -90 <= lat <= 90 and -180 <= lon <= 180

    async def geocode_event(event):
        loc = event.get("location", {})
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        
        if lat is not None and lon is not None and is_valid_lat_lon(lat, lon):
            return
            
        venue = loc.get("venue", "").strip()
        city = event.get("city", "").strip()
        if venue and city:
            async with semaphore:
                coords = await async_geocode_structured(venue, city)
            if coords:
                event["location"]["latitude"] = coords["lat"]
                event["location"]["longitude"] = coords["lon"]
            else:
                event["location"]["latitude"] = None
                event["location"]["longitude"] = None
        else:
            event["location"]["latitude"] = None
            event["location"]["longitude"] = None

    logger.info("🌍 Geocoding events (parallel, skipping valid coords)")
    await asyncio.gather(*(geocode_event(event) for event in events))

    geocoded_path = os.path.splitext(json_path)[0] + "_geocoded_structured.json"
    with open(geocoded_path, "w", encoding="utf-8") as f:
        json.dump(events_data, f, ensure_ascii=False, indent=2)

    # ✅ FREE TIER: Optimized collection
    ensure_collection_exists()

    # FREE TIER OPTIMIZED INGESTION
    BATCH_SIZE = 16  # ✅ Smaller batches for 1GB RAM limit
    inserted = updated = skipped_unchanged = 0

    logger.info(f"⚡ Starting FREE TIER optimized ingestion ({len(events)} events, batch_size={BATCH_SIZE})")

    for start in tqdm(range(0, len(events), BATCH_SIZE), desc="Batches"):
        batch = events[start: start + BATCH_SIZE]
        texts = [normalize_text(event.get("description", "")) for event in batch]
        
        # Generate embeddings
        dense_embeddings = list(dense_embedding_model.passage_embed(texts))
        sparse_embeddings = list(sparse_embedding_model.passage_embed(texts))
        points = []

        for i, event in enumerate(batch):
            event_id = event.get("id")
            if not event_id:
                logger.warning(f"Skipping event without id")
                continue
                
            text = texts[i]
            chunk_hash = calculate_hash(text)

            # FREE TIER: Optimized existence check
            existing_points, _ = client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=models.Filter(
                    must=[models.FieldCondition(key="id", match=models.MatchValue(value=event_id))]
                ),
                limit=1,
            )
            
            if existing_points:
                existing_point = existing_points[0]
                existing_hash = existing_point.payload.get("hash")
                
                if existing_hash == chunk_hash:
                    skipped_unchanged += 1
                    continue
                else:
                    logger.debug(f"Updating {event_id}: hash changed")
                    point_id_to_use = existing_point.id
                    updated += 1
            else:
                inserted += 1
                point_id_to_use = str(uuid4())

            # Location payload
            loc = event.get("location", {})
            loc_geo = {}
            if "latitude" in loc and "longitude" in loc and loc["latitude"] and loc["longitude"]:
                loc_geo = {
                    "lat": float(loc["latitude"]),
                    "lon": float(loc["longitude"])
                }
            
            location_payload = {**loc, **loc_geo}
            payload = {
                **event, 
                "location": location_payload, 
                "hash": chunk_hash
            }

            # FREE TIER: Vector truncation for quantization
            dense_vec = dense_embeddings[i][:384].tolist()  # ✅ INT8 compatible
            sparse_vec = sparse_embeddings[i]
            
            points.append(
                models.PointStruct(
                    id=point_id_to_use,
                    vector={
                        DENSE_VECTOR_NAME: dense_vec,
                        SPARSE_VECTOR_NAME: models.SparseVector(
                            indices=list(sparse_vec.indices[:100]),  # ✅ Limit sparse indices
                            values=[float(v) for v in sparse_vec.values[:100]]
                        ),
                    },
                    payload=payload,
                )
            )

        if points:
            try:
                # FREE TIER: wait=False for speed
                client.upsert(
                    collection_name=COLLECTION_NAME, 
                    points=points, 
                    wait=False  # ✅ Fire-and-forget
                )
            except Exception as e:
                logger.error(f"Batch {start//BATCH_SIZE + 1} failed: {e}")

    collection_info = client.get_collection(COLLECTION_NAME)
    
    logger.info(f"🎉 FREE TIER Ingestion complete!")
    logger.info(f"   ➕ Inserted: {inserted}")
    logger.info(f"   ✏️  Updated: {updated}")
    logger.info(f"   ⏭️  Skipped: {skipped_unchanged}")
    logger.info(f"   📊 Points in collection: {collection_info.points_count}")
    
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped_unchanged": skipped_unchanged,
        "collection_info": collection_info,
        "batches_processed": (len(events) + BATCH_SIZE - 1) // BATCH_SIZE,
        "optimized": True
    }
