from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from app.services.csv_delta_service import compute_csv_delta
from app.services.json_delta_service import compute_json_delta  # <--- NEW SERVICE IMPORT
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
from app.services import tm_service 

# Logging Setup
logger = logging.getLogger(__name__)
router = APIRouter()

# ---------- DATASET DIRECTORY CONFIGURATION ----------
if Path("/app/dataset").exists():
    DATASET_DIR = Path("/app/dataset")
elif Path("/dataset").exists():
    DATASET_DIR = Path("/dataset")
else:
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

# --- UNPLI SCRAPER & INGESTOR ---

@router.get("/scrape_unpli_events")
async def trigger_unpli_scrape(
    page_no: int = Query(1), 
    page_size: int = Query(500),
    session_id: str = Query(None)
):
    """
    Orchestrates the UNPLI scrape by calling the fetching and transformation 
    logic defined in app/services/scrape.py.
    """
    try:
        current_session_id = session_id or UNPLI_SESSION_ID
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # 1. Fetch raw events
            logger.info(f"📡 Fetching UNPLI page {page_no} (size {page_size})...")
            raw_events = await scrape.fetch_unpli_events(
                session=client,
                page_no=page_no,
                page_size=page_size,
                session_id=current_session_id
            )
            
            if not raw_events:
                return {"status": "error", "message": "No events returned from API", "events": []}

            # 2. Transform events for JSON format
            logger.info(f"⚙️ Transforming {len(raw_events)} raw events...")
            transformed_events = await scrape.transform_events_for_json(
                events=raw_events,
                session_id=current_session_id
            )
            
            # 3. Save to disk (updated to clean YYYY-MM-DD format)
            output_file = DATASET_DIR / f"unpli_events_{datetime.now().strftime('%Y-%m-%d')}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump({"events": transformed_events}, f, indent=2, ensure_ascii=False)

            return {
                "status": "success",
                "count": len(transformed_events),
                "file_saved": str(output_file),
                "events": transformed_events
            }
            
    except Exception as e:
        logger.error(f"❌ Scraping orchestration failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ingest-unpli-delta")
async def ingest_unpli_delta():
    """
    Compares unpli_current.json and unpli_last.json automatically,
    calculates added/changed/removed via json_delta_service,
    and ingests only the differences into Qdrant.
    """
    try:
        current_path = DATASET_DIR / "unpli_current.json"
        last_path = DATASET_DIR / "unpli_last.json"

        if not current_path.exists():
            raise HTTPException(status_code=404, detail="unpli_current.json not found in dataset folder.")

        # 1. Compute delta using the external service
        logger.info("🔍 Computing JSON Delta...")
        delta_events = compute_json_delta(last_path, current_path)

        if not delta_events:
            return {"status": "skipped", "message": "No changes detected between files", "inserted": 0, "updated": 0, "deleted": 0}

        # 2. Ingest the resulting delta list
        logger.info(f"🚀 Ingesting {len(delta_events)} delta events into Qdrant...")
        result = await ingest_events_into_qdrant(delta_events)
        
        return {"status": "success", "delta_count": len(delta_events), **result}
    except Exception as e:
        logger.error(f"❌ UNPLI Delta Ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- TicketMaster ---
@router.post("/ingest-tm-delta")
async def ingest_tm_delta(country: str = Query("IT")):
    """
    Endpoint per il dump di Ticketmaster.
    Funziona sia per --initialize che per delta giornalieri.
    """
    try:
        # Percorsi file grezzi (scaricati dallo script .sh)
        current_raw = DATASET_DIR / f"tm_current_{country}.json"
        
        # Percorsi file standardizzati (per il confronto delta)
        current_std = DATASET_DIR / f"tm_std_{country}_current.json"
        last_std = DATASET_DIR / f"tm_std_{country}_last.json"

        if not current_raw.exists():
            raise HTTPException(status_code=404, detail=f"File {current_raw.name} non trovato.")

        # 1. TRASFORMAZIONE
        logger.info(f"⚙️ Trasformazione in corso per {country}...")
        standardized_events = tm_service.load_and_transform_tm_file(current_raw)
        
        with open(current_std, "w", encoding="utf-8") as f:
            json.dump({"events": standardized_events}, f, indent=2)

        # 2. CALCOLO DELTA
        # Se last_std non esiste (es. --initialize), compute_json_delta 
        # segnerà tutto come 'added'.
        logger.info(f"🔍 Calcolo delta rispetto a sessione precedente...")
        delta_events = compute_json_delta(last_std, current_std)

        if not delta_events:
            return {"status": "skipped", "message": "Nessun cambiamento rilevato."}

        # 3. INGESTIONE
        logger.info(f"🚀 Ingestione di {len(delta_events)} eventi delta in Qdrant...")
        result = await ingest_events_into_qdrant(delta_events)
        
        # 4. ROTAZIONE FILE STANDARD
        # Prepariamo il file per il confronto di domani
        if current_std.exists():
            import shutil
            shutil.copy(str(current_std), str(last_std))

        return {
            "status": "success", 
            "country": country, 
            "total_processed": len(standardized_events),
            "delta_applied": len(delta_events),
            **result 
        }

    except Exception as e:
        logger.error(f"❌ Errore pipeline TM: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- TICKET SQUEEZE DELTA PIPELINE ---

@router.post("/compute-delta")
async def compute_csv_delta_endpoint(old_file: UploadFile = File(...), new_file: UploadFile = File(...), keys: str = "event_id"):
    """Computes the CSV difference for TicketSqueeze pipeline."""
    try:
        old_content = await old_file.read()
        new_content = await new_file.read()
        result = compute_csv_delta(old_csv_content=old_content, new_csv_content=new_content, keys=keys, output_path=DATASET_DIR / "delta.csv")
        return {"status": "success", "summary": result["summary"], "csv_path": str(result["csv_path"])}
    except Exception as e:
        logger.error(f"Delta computation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ticketsqueeze/process-delta")
async def process_ticketsqueeze_delta(
    file: UploadFile = File(...), 
    include_removed: bool = Query(True), 
    include_changed: bool = Query(True)
):
    """Transforms delta CSV to JSON for TicketSqueeze."""
    try:
        temp_csv_path = DATASET_DIR / file.filename
        with open(temp_csv_path, "wb") as f:
            f.write(await file.read())
        
        result = await ticketsqueeze.process_ticketsqueeze_daily_delta(
            delta_csv_path=temp_csv_path, 
            include_removed=include_removed, 
            include_changed=include_changed
        )
        
        output_json_path = DATASET_DIR / "ts_delta_delta.json"
        ticketsqueeze.save_events_to_json(result["events"], output_json_path)
        
        return {
            "status": "success", 
            "summary": result["summary"], 
            "saved_path": str(output_json_path)
        }
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/ingestticketsqueezedelta")
async def ingest_ticketsqueeze_delta(file: UploadFile = File(...)):
    """Final ingestion step for TicketSqueeze JSON delta."""
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files accepted")

    try:
        save_path = DATASET_DIR / file.filename
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        with open(save_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        events = data if isinstance(data, list) else data.get("events", [])
        result = await ingest_events_into_qdrant(events)
        
        return {"status": "success", "filename": file.filename, **result}
    except Exception as e:
        logger.error(f"❌ TicketSqueeze Ingestion crashed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# --- CORE SEARCH & UTILITIES ---

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
                raise HTTPException(status_code=400, detail="Route must contain two different points.")
            base_geometry = LineString(route_coords)
        else:
            base_geometry = origin_point_sh

        gdf = gpd.GeoDataFrame([{"geometry": base_geometry}], crs="EPSG:4326")
        gdf_3857 = gdf.to_crs(epsg=3857)
        buffer_distance_meters = request.buffer_distance * 1000
        buffer_polygon = gdf_3857.buffer(buffer_distance_meters).to_crs(epsg=4326).iloc[0]

        # 🔥 SINGLE LINE MULTIPOLYGON FIX:
        if buffer_polygon.geom_type == 'MultiPolygon':
            buffer_polygon = max(buffer_polygon.geoms, key=lambda p: p.area)

        #print(f"🔍 FIXED buffer type: {buffer_polygon.geom_type}")  # Shows "Polygon"

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

@router.delete("/cleanup-past-events")
async def cleanup_past_events(dry_run: bool = True, max_scan: int = 50000):
    """Utility to prune old events from Qdrant."""
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
        
        if not offset or (max_scan and deleted >= max_scan): break
        
    return {"status": "success", "deleted": deleted, "dry_run": dry_run}