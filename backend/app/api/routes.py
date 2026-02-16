from fastapi import APIRouter, HTTPException, UploadFile, File
from app.services.csv_delta_service import compute_csv_delta
from app.services.ingest_service import ingest_events_into_qdrant, COLLECTION_NAME
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
from datetime import datetime, timezone
from pathlib import Path
from shapely.geometry import LineString, Point
import numpy as np
import geopandas as gpd
from app.core.config import (
    DENSE_MODEL_NAME,
    SPARSE_MODEL_NAME,
    QDRANT_SERVER,
    QDRANT_API_KEY,
    UNPLI_SESSION_ID,
)
from fastembed import TextEmbedding, SparseTextEmbedding
import os
import json
import shutil
import httpx
import logging

# Logging Setup
logger = logging.getLogger(__name__)
router = APIRouter()

# ---------- DATASET DIRECTORY CONFIGURATION ----------
if Path("/app/dataset").exists():
    DATASET_DIR = Path("/app/dataset")
elif Path("/dataset").exists():
    DATASET_DIR = Path("/dataset")
else:
    # FIXED: .parents[3] goes from backend/app/api/routes.py up to the remap/ root
    DATASET_DIR = Path(__file__).resolve().parents[3] / "dataset"

DATASET_DIR.mkdir(parents=True, exist_ok=True)
# -----------------------------------------------------

# Shared AI Models (Initialized with 1 thread for memory safety)
dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME, threads=1)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME, threads=1)

# ---------- ENDPOINTS ----------

@router.get("/collection_info")
async def get_collection_info():
    client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY)
    if not client.collection_exists(COLLECTION_NAME):
        return {"error": f"Collection {COLLECTION_NAME} does not exist"}
    info = client.get_collection(COLLECTION_NAME)
    return {
        "collection": COLLECTION_NAME,
        "points_count": info.points_count,
        "status": str(info.status)
    }

@router.post("/ingestevents")
async def ingest_events_endpoint(file: UploadFile = File(...)):
    """
    ✅ RESTORED: Standard ingestion for ingest.sh
    Delegates to service for memory-optimized processing.
    """
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted")

    save_path = DATASET_DIR / file.filename
    try:
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        with open(save_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Handle different JSON structures (list or object with 'events' key)
        events = data if isinstance(data, list) else data.get("events", [])
        
        # Use the optimized service logic
        result = await ingest_events_into_qdrant(events)
        return result
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ingestticketsqueezedelta")
async def ingest_ticketsqueeze_delta(file: UploadFile = File(...)):
    """
    MEMORY OPTIMIZED TICKET SQUEEZE INGESTION
    """
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files accepted")

    save_path = DATASET_DIR / file.filename
    try:
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        with open(save_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        events = data if isinstance(data, list) else data.get("events", [])
        result = await ingest_events_into_qdrant(events)
        
        return {
            "status": "success",
            "filename": file.filename,
            **result
        }
    except Exception as e:
        logger.error(f"❌ TicketSqueeze Ingestion crashed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/create_map")
async def create_event_map(request: schemas.RouteRequest):
    try:
        origin_lon, origin_lat = geocode_address(request.origin_address)
        origin_point_sh = Point(origin_lon, origin_lat)
        route_coords = []
        destination_data = None
        
        if request.destination_address:
            dest_lon, dest_lat = geocode_address(request.destination_address)
            destination_data = {"lat": dest_lat, "lon": dest_lon, "address": request.destination_address}
            coords = [[origin_lon, origin_lat], [dest_lon, dest_lat]]
            routes = get_route(coords, profile=request.profile_choice)
            route_geometry = routes["features"][0]["geometry"]
            route_coords = route_geometry["coordinates"]
            if len(route_coords) < 2:
                raise HTTPException(status_code=400, detail="Route must contain two different addresses.")
            base_geometry = LineString(route_coords)
        else:
            base_geometry = origin_point_sh

        gdf = gpd.GeoDataFrame([{"geometry": base_geometry}], crs="EPSG:4326")
        gdf_3857 = gdf.to_crs(epsg=3857)
        buffer_distance_meters = request.buffer_distance * 1000
        buffer_polygon = gdf_3857.buffer(buffer_distance_meters).to_crs(epsg=4326).iloc[0]
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

        if payloads:
            def distance_logic(event):
                event_pt = Point(event["location"]["lon"], event["location"]["lat"])
                return base_geometry.project(event_pt) if request.destination_address else origin_point_sh.distance(event_pt)

            sorted_events = sorted(payloads, key=distance_logic)
            for event in sorted_events:
                loc = event.get('location', {})
                event['lat'], event['lon'], event['address'] = loc.get('lat'), loc.get('lon'), loc.get('address')
        else:
            sorted_events = []

        return {
            "route_coords": route_coords,
            "buffer_polygon": polygon_coords,
            "origin": {"lat": origin_lat, "lon": origin_lon, "address": request.origin_address},
            "destination": destination_data,
            "events": sorted_events,
            "mode": "route" if request.destination_address else "point",
            "message": "No events found" if not payloads else None
        }
    except Exception as e:
        logger.error(f"Error in create_map: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sentencetopayload")
async def sentence_to_payload(data: SentenceInput):
    try:
        output = extract_payload(data.sentence)
        return output.model_dump() if hasattr(output, "model_dump") else output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/scrape_unpli_events")
async def scrape_unpli_events(page_no: int = 1, page_size: int = 10, session_id: str = None):
    final_session_id = session_id or UNPLI_SESSION_ID
    async with httpx.AsyncClient() as session:
        events = await scrape.fetch_unpli_events(session, page_no=page_no, page_size=page_size, session_id=final_session_id)
        if not events: raise HTTPException(status_code=404, detail="No events found")
        transformed = await scrape.transform_events_for_json(events, session_id=final_session_id)
        save_path = DATASET_DIR / f"unpli_events_{page_no}.json"
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump({"events": transformed}, f, ensure_ascii=False, indent=4)
        return {"events": transformed, "saved_path": str(save_path)}

@router.post("/compute-delta")
async def compute_csv_delta_endpoint(old_file: UploadFile = File(...), new_file: UploadFile = File(...), keys: str = "event_id"):
    try:
        old_content, new_content = await old_file.read(), await new_file.read()
        result = compute_csv_delta(old_csv_content=old_content, new_csv_content=new_content, keys=keys, output_path=DATASET_DIR / "delta.csv")
        return {"status": "success", "summary": result["summary"], "csv_path": str(result["csv_path"])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/processticketsqueezedelta")
async def process_ticketsqueeze_delta(file: UploadFile = File(...), include_removed: bool = False, include_changed: bool = True):
    try:
        temp_csv_path = DATASET_DIR / f"temp_{file.filename}"
        with open(temp_csv_path, "wb") as f: f.write(await file.read())
        result = await ticketsqueeze.process_ticketsqueeze_daily_delta(delta_csv_path=temp_csv_path, include_removed=include_removed, include_changed=include_changed)
        output_json_path = DATASET_DIR / f"ts_delta_{file.filename.replace('.csv', '.json')}"
        ticketsqueeze.save_events_to_json(result["events"], output_json_path)
        if temp_csv_path.exists(): temp_csv_path.unlink()
        return {"status": "success", "summary": result["summary"], "saved_path": str(output_json_path)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/cleanup-past-events")
async def cleanup_past_events(dry_run: bool = True, max_scan: int = 50000, quick_delete: bool = False):
    client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY)
    today = datetime.now(timezone.utc).date()
    deleted = 0
    
    offset = None
    while True:
        result = client.scroll(collection_name=COLLECTION_NAME, limit=100, with_payload=True, offset=offset)
        points, offset = result
        if not points: break
        
        batch_to_del = []
        for p in points:
            ds = p.payload.get("start_date")
            if ds:
                try:
                    pdate = datetime.fromisoformat(ds.replace('Z', '+00:00')).date()
                    if pdate < today: batch_to_del.append(p.id)
                except: continue
        
        if batch_to_del and not dry_run:
            client.delete(collection_name=COLLECTION_NAME, points_selector=models.PointIdsList(points=batch_to_del))
            deleted += len(batch_to_del)
        
        if not offset or deleted > max_scan: break
        
    return {"status": "success", "deleted": deleted, "dry_run": dry_run}