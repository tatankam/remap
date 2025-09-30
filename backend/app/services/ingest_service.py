import os
import json
import asyncio
import hashlib
import logging
from uuid import uuid4
from typing import Optional, Dict, Any

import httpx
from dotenv import load_dotenv
from tqdm import tqdm
from fastembed import TextEmbedding, SparseTextEmbedding
from qdrant_client import QdrantClient, models
from app.core.config import QDRANT_SERVER, QDRANT_API_KEY, DENSE_MODEL_NAME, SPARSE_MODEL_NAME, COLLECTION_NAME

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not QDRANT_SERVER or not QDRANT_API_KEY:
    raise EnvironmentError("QDRANT_SERVER or QDRANT_API_KEY not defined in .env file")

dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME)
client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=200000)

DENSE_VECTOR_NAME = "dense_vector"
SPARSE_VECTOR_NAME = "sparse_vector"


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
            except (httpx.HTTPError, ValueError) as e:
                logger.warning(f"Geocoding error with params {params}: {e}")
            await asyncio.sleep(1)  # Respect Nominatim usage policy
    return None


def calculate_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ensure_collection_exists():
    example_text = "Test for embedding dimension calculation."
    dense_emb = list(dense_embedding_model.passage_embed([example_text]))[0]
    dense_dim = len(dense_emb)
    if not client.collection_exists(COLLECTION_NAME):
        logger.info(f"Creating collection {COLLECTION_NAME} with dimension {dense_dim}")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config={
                DENSE_VECTOR_NAME: models.VectorParams(size=dense_dim, distance=models.Distance.COSINE),
            },
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: models.SparseVectorParams(),
            }
        )
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
        except Exception as e:
            logger.debug(f"Payload index for {field_name} might already exist or error: {e}")


async def ingest_events_from_file(json_path: str) -> Dict[str, Any]:
    logger.info(f"Loading events from {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        events_data = json.load(f)

    events = events_data.get("events", [])

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
        logger.info(f"Checking coordinates for event ID {event.get('id')} lat={lat}, lon={lon}")
        if lat is not None and lon is not None and is_valid_lat_lon(lat, lon):
            # Skip geocoding since valid coordinates exist
            logger.info(f"Skipping geocoding for event ID {event.get('id')}")
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

    logger.info("Geocoding events asynchronously")
    await asyncio.gather(*(geocode_event(event) for event in events))

    geocoded_path = os.path.splitext(json_path)[0] + "_geocoded_structured.json"
    logger.info(f"Saving geocoded events to {geocoded_path}")
    with open(geocoded_path, "w", encoding="utf-8") as f:
        json.dump(events_data, f, ensure_ascii=False, indent=2)

    ensure_collection_exists()

    BATCH_SIZE = 32
    inserted = 0
    updated = 0
    skipped_unchanged = 0

    for start in tqdm(range(0, len(events), BATCH_SIZE)):
        batch = events[start: start + BATCH_SIZE]
        texts = [event.get("description", "") for event in batch]
        dense_embeddings = list(dense_embedding_model.passage_embed(texts))
        sparse_embeddings = list(sparse_embedding_model.passage_embed(texts))
        points = []

        for i, event in enumerate(batch):
            event_id = event.get("id")
            if not event_id:
                logger.warning(f"Skipping event without id: {event}")
                continue
            text = texts[i]
            chunk_hash = calculate_hash(text)

            existing_points, _ = client.scroll(
                collection_name=COLLECTION_NAME,
                scroll_filter=models.Filter(
                    must=[models.FieldCondition(key="id", match=models.MatchValue(value=event_id))]
                ),
                limit=1,
            )

            if existing_points:
                existing_point = existing_points[0]
                existing_hash = existing_point.payload.get("hash", "")
                if existing_hash == chunk_hash:
                    skipped_unchanged += 1
                    continue
                else:
                    client.delete(
                        collection_name=COLLECTION_NAME,
                        points_selector=models.PointIdsList(points=[existing_point.id]),
                    )
                    updated += 1
            else:
                inserted += 1

            loc = event.get("location", {})
            loc_geo = {}
            if "latitude" in loc and "longitude" in loc:
                loc_geo = {"lat": loc["latitude"], "lon": loc["longitude"]}

            location_payload = {**loc, **loc_geo}  # Merges original location dict with lat/lon keys

            payload = {**event, "location": location_payload, "hash": chunk_hash}

            points.append(
                models.PointStruct(
                    id=str(uuid4()),
                    vector={
                        DENSE_VECTOR_NAME: dense_embeddings[i].tolist(),
                        SPARSE_VECTOR_NAME: models.SparseVector(
                            indices=list(sparse_embeddings[i].indices),
                            values=list(sparse_embeddings[i].values),
                        ),
                    },
                    payload=payload,
                )
            )

        if points:
            try:
                client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)
            except Exception as e:
                logger.error(f"Error uploading points batch: {e}")

    collection_info = client.get_collection(COLLECTION_NAME)
    logger.info(f"Ingestion complete: inserted={inserted}, updated={updated}, skipped={skipped_unchanged}")
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped_unchanged": skipped_unchanged,
        "collection_info": collection_info,
    }
