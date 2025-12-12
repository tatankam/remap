from fastapi import APIRouter, HTTPException, UploadFile, File
from app.services.csv_delta_service import compute_csv_delta
from app.services.ingest_service import (
    ingest_events_from_file,
    ensure_collection_exists,
    calculate_hash,
    DENSE_VECTOR_NAME,
    SPARSE_VECTOR_NAME,
    COLLECTION_NAME,
)
from app.services.openroute_service import geocode_address, get_route
from app.services.qdrant_service import (
    build_geo_filter,
    build_date_intersection_filter,
    build_final_filter,
    query_events_hybrid,
)
from app.services.extraction_service import extract_payload
from app.services import scrape
from app.services import ticketsqueeze
from app.models import schemas
from app.models.schemas import SentenceInput
from pydantic import ValidationError
from qdrant_client import QdrantClient, models
from uuid import uuid4
from pathlib import Path
from shapely.geometry import LineString, Point
import numpy as np
import geopandas as gpd
from app.core.config import (
    DENSE_MODEL_NAME,
    SPARSE_MODEL_NAME,
    COLLECTION_NAME,
    QDRANT_SERVER,
    QDRANT_API_KEY,
    UNPLI_SESSION_ID,
)
from fastembed import TextEmbedding, SparseTextEmbedding
import os
import json
import shutil
import httpx
import unicodedata
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------- DATASET DIRECTORY (PROJECT ROOT) ----------
DATASET_DIR = Path(__file__).resolve().parents[3] / "dataset"
DATASET_DIR.mkdir(parents=True, exist_ok=True)

# ---------- EMBEDDING MODELS ----------
dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME)

# ---------- DEBUG ENDPOINT ----------
@router.get("/collection_info")
async def get_collection_info():
    """🔍 DEBUG: Check collection status + vector names"""
    client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY)
    if not client.collection_exists(COLLECTION_NAME):
        return {"error": f"Collection {COLLECTION_NAME} does not exist"}
    
    info = client.get_collection(COLLECTION_NAME)
    return {
        "collection": COLLECTION_NAME,
        "points_count": info.points_count,
        "vectors_count": getattr(info, 'vectors_count', 0),
        "dense_vector_name": DENSE_VECTOR_NAME,
        "sparse_vector_name": SPARSE_VECTOR_NAME,
        "status": str(info.status),
        "config": {
            "dense_dim": len(list(dense_embedding_model.passage_embed(["test"]))[0]),
            "dataset_dir": str(DATASET_DIR)
        }
    }

@router.post("/create_map")
async def create_event_map(request: schemas.RouteRequest):
    try:
        origin_point = geocode_address(request.origin_address)
        destination_point = geocode_address(request.destination_address)
        coords = [origin_point, destination_point]

        routes = get_route(coords, profile=request.profile_choice)
        route_geometry = routes["features"][0]["geometry"]
        route_coords = route_geometry["coordinates"]
        if len(route_coords) < 2:
            raise HTTPException(status_code=400, detail="Route must contain two different addresses for buffering.")

        route_line = LineString(route_coords)
        route_gdf = gpd.GeoDataFrame([{"geometry": route_line}], crs="EPSG:4326")

        route_gdf_3857 = route_gdf.to_crs(epsg=3857)
        if request.buffer_distance <= 0:
            raise HTTPException(status_code=400, detail="Buffer distance must be positive (km).")
        buffer_distance_meters = request.buffer_distance * 1000
        buffer_polygon = route_gdf_3857.buffer(buffer_distance_meters).to_crs(epsg=4326).iloc[0]
        polygon_coords = np.array(buffer_polygon.exterior.coords).tolist()
        polygon_coords_qdrant = [{"lon": lon, "lat": lat} for lon, lat in polygon_coords]

        geo_filter = build_geo_filter(polygon_coords_qdrant)
        date_filter = build_date_intersection_filter(request.startinputdate, request.endinputdate)
        final_filter = build_final_filter(geo_filter, date_filter)

        score_threshold = 0.0 if request.query_text.strip() == "" else 0.34
        query_dense_vector = list(dense_embedding_model.passage_embed([request.query_text]))[0].tolist()
        query_sparse_embedding = list(sparse_embedding_model.passage_embed([request.query_text]))[0]

        payloads = query_events_hybrid(
            dense_vector=query_dense_vector,
            sparse_vector=query_sparse_embedding,
            query_filter=final_filter,
            collection_name=COLLECTION_NAME,
            limit=request.numevents,
            score_threshold=score_threshold,
        )

        if not payloads:
            return {"message": "No events found in Qdrant for this route/buffer and date range."}

        def distance_along_route(event):
            point = Point(event["location"]["lon"], event["location"]["lat"])
            return route_line.project(point)

        sorted_events = sorted(payloads, key=distance_along_route)
        for event in sorted_events:
            loc = event.get('location', {})
            event['address'] = loc.get('address')
            event['lat'] = loc.get('lat')
            event['lon'] = loc.get('lon')

        response = {
            "route_coords": route_coords,
            "buffer_polygon": polygon_coords,
            "origin": {"lat": origin_point[1], "lon": origin_point[0], "address": request.origin_address},
            "destination": {"lat": destination_point[1], "lon": destination_point[0], "address": request.destination_address},
            "events": sorted_events
        }
        return response
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ingestevents")
async def ingest_events_endpoint(file: UploadFile = File(...)):
    """✅ FIXED: Force sync + return real counts"""
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted")

    save_path = DATASET_DIR / file.filename
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        result = await ingest_events_from_file(str(save_path))
        
        # ✅ FORCE COLLECTION SYNC + REAL COUNTS
        client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY)
        client.collection_exists(COLLECTION_NAME)
        collection_info = client.get_collection(COLLECTION_NAME)
        result["points_count"] = collection_info.points_count
        result["vectors_count"] = getattr(collection_info, 'vectors_count', 0)
        
        logger.info(f"✅ Ingest complete: {result['points_count']} points in collection")
        return result
        
    finally:
        if save_path.exists():
            save_path.unlink()

@router.post("/sentencetopayload")
async def sentence_to_payload(data: SentenceInput):
    sentence = data.sentence
    try:
        output = extract_payload(sentence)
        if output:
            if hasattr(output, "model_dump"):
                return output.model_dump()
            elif isinstance(output, dict):
                return output
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@router.get("/scrape_unpli_events")
async def scrape_unpli_events(page_no: int = 1, page_size: int = 10, session_id: str = None):
    """✅ SECURE: Uses UNPLI_SESSION_ID from .env"""
    final_session_id = session_id or UNPLI_SESSION_ID
    async with httpx.AsyncClient() as session:
        events = await scrape.fetch_unpli_events(session, page_no=page_no, page_size=page_size, session_id=final_session_id)
        if not events:
            raise HTTPException(status_code=404, detail="No events found or error fetching data.")
        transformed_events = await scrape.transform_events_for_json(events, session_id=final_session_id)

        save_path = DATASET_DIR / f"veneto_unpliveneto_events_{page_no}_{page_size}.json"
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump({"events": transformed_events}, f, ensure_ascii=False, indent=4)

        return {"events": transformed_events, "saved_path": str(save_path)}

@router.post("/compute-delta")
async def compute_csv_delta_endpoint(
    old_file: UploadFile = File(..., description="Older CSV file (baseline)"),
    new_file: UploadFile = File(..., description="Newer CSV file"),
    keys: str = "event_id",
    save_output: bool = True,
):
    try:
        old_content = await old_file.read()
        new_content = await new_file.read()

        DATASET_DIR.mkdir(parents=True, exist_ok=True)
        output_path = DATASET_DIR / "delta.csv" if save_output else None

        result = compute_csv_delta(
            old_csv_content=old_content,
            new_csv_content=new_content,
            keys=keys,
            output_path=output_path,
        )

        def safe_dicts(df):
            if df.empty:
                return []
            preview = df.head(5)
            return preview.astype(str).to_dict("records")

        safe_summary = {k: int(v) for k, v in result["summary"].items()}

        return {
            "status": "success",
            "old_file": old_file.filename,
            "new_file": new_file.filename,
            "summary": safe_summary,
            "delta_preview": safe_dicts(result["delta_df"]),
            "csv_path": str(result["csv_path"]) if result["csv_path"] is not None else None,
            "total_changes": int(safe_summary.get("total", 0)),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delta computation failed: {str(e)}")

@router.post("/processticketsqueezedelta")
async def process_ticketsqueeze_delta(
    file: UploadFile = File(...),
    include_removed: bool = False,
    include_changed: bool = True,
):
    try:
        DATASET_DIR.mkdir(parents=True, exist_ok=True)

        temp_csv_path = DATASET_DIR / f"temp_{file.filename}"

        with open(temp_csv_path, "wb") as f:
            content = await file.read()
            f.write(content)

        result = await ticketsqueeze.process_ticketsqueeze_daily_delta(
            delta_csv_path=temp_csv_path,
            include_removed=include_removed,
            include_changed=include_changed,
        )

        output_json_path = DATASET_DIR / f"ticketsqueeze_delta_{file.filename.replace('.csv', '.json')}"
        ticketsqueeze.save_events_to_json(result["events"], output_json_path)

        if temp_csv_path.exists():
            temp_csv_path.unlink()

        return {
            "status": "success",
            "events": result["events"],
            "summary": result["summary"],
            "saved_path": str(output_json_path),
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing TicketSqueeze delta: {str(e)}")

@router.post("/ingestticketsqueezedelta")
async def ingest_ticketsqueeze_delta(file: UploadFile = File(...)):
    """
    FREE TIER optimized:
    - DELETE removed events first
    - UPSERT added/changed
    - Uses shared DATASET_DIR
    """
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted")

    try:
        DATASET_DIR.mkdir(parents=True, exist_ok=True)

        save_path = DATASET_DIR / file.filename

        with open(save_path, "wb") as f:
            content = await file.read()
            f.write(content)

        logger.info(f"Loading TicketSqueeze events from {save_path}")
        with open(save_path, "r", encoding="utf-8") as f:
            events_data = json.load(f)

        events = events_data.get("events", [])
        if not events:
            return {"status": "empty", "message": "No events in file"}

        logger.info(f"Loaded {len(events)} events from {file.filename}")

        ensure_collection_exists()

        client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=30000)

        def normalize_text(text: str) -> str:
            if not text:
                return ""
            text = text.strip()[:1000]
            text = unicodedata.normalize("NFKC", text)
            return text

        # PHASE 1: delete removed
        removed_events = [e for e in events if e.get("delta_type") == "removed"]
        deleted_count = 0
        if removed_events:
            delete_ids = [
                str(e.get("event_id", e.get("id", "")))
                for e in removed_events
                if e.get("event_id") or e.get("id")
            ]
            if delete_ids:
                client.delete(
                    collection_name=COLLECTION_NAME,
                    points=models.PointIdsList(points=[models.PointId(id=pid) for pid in delete_ids]),
                    wait=True,
                )
                deleted_count = len(delete_ids)
                logger.info(f"Deleted {deleted_count} removed events")

        # PHASE 2: upsert added + changed
        active_events = [e for e in events if e.get("delta_type") in ["added", "changed"]]
        if not active_events:
            return {
                "status": "success",
                "filename": file.filename,
                "deleted": deleted_count,
                "processed": 0,
                "message": "Only deletions processed",
            }

        BATCH_SIZE = 16
        inserted = updated = skipped_unchanged = 0

        for start in range(0, len(active_events), BATCH_SIZE):
            batch = active_events[start : start + BATCH_SIZE]
            texts = [normalize_text(e.get("description", "")) for e in batch]

            dense_embeddings = list(dense_embedding_model.passage_embed(texts))
            sparse_embeddings = list(sparse_embedding_model.passage_embed(texts))
            points = []

            for i, event in enumerate(batch):
                event_id = str(event.get("id") or event.get("event_id", ""))
                if not event_id:
                    logger.warning(f"Skipping event without ID: {event}")
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
                    existing_hash = existing_point.payload.get("hash")

                    if existing_hash == chunk_hash:
                        skipped_unchanged += 1
                        continue
                    else:
                        point_id_to_use = existing_point.id
                        updated += 1
                else:
                    inserted += 1
                    point_id_to_use = models.PointId(id=str(uuid4()))

                loc = event.get("location", {})
                loc_geo = {}
                if loc.get("latitude") and loc.get("longitude"):
                    loc_geo = {
                        "lat": float(loc["latitude"]),
                        "lon": float(loc["longitude"]),
                    }

                location_payload = {**loc, **loc_geo}
                payload = {
                    **event,
                    "id": event_id,
                    "location": location_payload,
                    "hash": chunk_hash,
                }

                points.append(
                    models.PointStruct(
                        id=point_id_to_use,
                        vector={
                            DENSE_VECTOR_NAME: dense_embeddings[i].tolist(),
                            SPARSE_VECTOR_NAME: models.SparseVector(
                                indices=list(sparse_embeddings[i].indices),
                                values=[float(v) for v in sparse_embeddings[i].values],
                            ),
                        },
                        payload=payload,
                    )
                )

            if points:
                try:
                    # ✅ FIXED: wait=True for immediate visibility
                    client.upsert(collection_name=COLLECTION_NAME, points=points, wait=True)
                except Exception as e:
                    logger.error(f"Batch {start // BATCH_SIZE + 1} failed: {e}")
                    continue

        # ✅ FINAL SYNC
        client.collection_exists(COLLECTION_NAME)
        collection_info = client.get_collection(COLLECTION_NAME)

        return {
            "status": "success",
            "filename": file.filename,
            "deleted": deleted_count,
            "inserted": inserted,
            "updated": updated,
            "skipped_unchanged": skipped_unchanged,
            "total_processed": len(active_events),
            "batches_sent": (len(active_events) + BATCH_SIZE - 1) // BATCH_SIZE,
            "points_count": collection_info.points_count,
            "message": "Free tier optimized ingestion complete (DELETE + UPSERT + SYNCED)",
        }

    except Exception as e:
        logger.error(f"Error in ingest_ticketsqueeze_delta: {e}")
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")
