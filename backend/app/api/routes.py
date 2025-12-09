from fastapi import APIRouter, HTTPException, UploadFile, File
from app.services.ingest_service import ingest_events_from_file
from app.services.openroute_service import geocode_address, get_route
from app.services.qdrant_service import (
    build_geo_filter,
    build_date_intersection_filter,
    build_final_filter,
    query_events_hybrid,
)
from app.services.extraction_service import extract_payload
from app.services import scrape  # new import for the scraper service
from app.services import ticketsqueeze  # new import for the ticketsqueeze service
from app.models import schemas
from app.models.schemas import SentenceInput
from pydantic import ValidationError

from shapely.geometry import LineString, Point
from pathlib import Path
import numpy as np
import geopandas as gpd
from qdrant_client.http import models as qmodels
from app.core.config import DENSE_MODEL_NAME, SPARSE_MODEL_NAME, COLLECTION_NAME
from fastembed import TextEmbedding, SparseTextEmbedding
import os, json
import shutil
import httpx
import pathlib
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize embedding models once for reuse
dense_embedding_model = TextEmbedding(DENSE_MODEL_NAME)
sparse_embedding_model = SparseTextEmbedding(SPARSE_MODEL_NAME)


@router.post("/create_map")
async def create_event_map(request: schemas.RouteRequest):
    try:
        origin_point = geocode_address(request.origin_address)
        destination_point = geocode_address(request.destination_address)
        coords = [origin_point, destination_point]

        routes = get_route(coords, profile=request.profile_choice)
        route_geometry = routes['features'][0]['geometry']
        route_coords = route_geometry['coordinates']
        if len(route_coords) < 2:
            raise HTTPException(status_code=400, detail="Route must contain two different address for buffering.")

        route_line = LineString(route_coords)
        route_gdf = gpd.GeoDataFrame([{'geometry': route_line}], crs='EPSG:4326')

        route_gdf_3857 = route_gdf.to_crs(epsg=3857)
        # Ensure buffer_distance is in kilometers; convert to meters for buffering
        if request.buffer_distance <= 0:
            raise HTTPException(status_code=400, detail="Buffer distance must be a positive number representing kilometers.")
        buffer_distance_meters = request.buffer_distance * 1000  # Convert km to meters
        buffer_polygon = route_gdf_3857.buffer(buffer_distance_meters).to_crs(epsg=4326).iloc[0]
        polygon_coords = np.array(buffer_polygon.exterior.coords).tolist()
        polygon_coords_qdrant = [{"lon": lon, "lat": lat} for lon, lat in polygon_coords]

        geo_filter = build_geo_filter(polygon_coords_qdrant)
        date_filter = build_date_intersection_filter(request.startinputdate, request.endinputdate)
        final_filter = build_final_filter(geo_filter, date_filter)

        score_threshold = 0.0 if request.query_text.strip() == "" else 0.34  # 0.34 is a balanced threshold, 0.0 if no text query

        query_dense_vector = list(dense_embedding_model.passage_embed([request.query_text]))[0].tolist()
        query_sparse_embedding = list(sparse_embedding_model.passage_embed([request.query_text]))[0]

        payloads = query_events_hybrid(
            dense_vector=query_dense_vector,
            sparse_vector=query_sparse_embedding,
            query_filter=final_filter,
            collection_name=COLLECTION_NAME,
            limit=request.numevents,
            score_threshold=score_threshold  # Optional: filter out low-score results
        )

        if not payloads:
            return {"message": "No events found in Qdrant for this route/buffer and date range."}

        def distance_along_route(event):
            point = Point(event['location']['lon'], event['location']['lat'])
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

    except HTTPException as e:
        # Re-raise HTTPExceptions (client errors)
        raise e


@router.post("/ingestevents")
async def ingest_events_endpoint(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted")

    save_dir = "/tmp"
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, file.filename)

    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        result = await ingest_events_from_file(save_path)
    finally:
        if os.path.exists(save_path):
            os.remove(save_path)

    return {
        "filename": file.filename,
        "inserted": result["inserted"],
        "updated": result["updated"],
        "skipped_unchanged": result["skipped_unchanged"],
        "collection_info": result["collection_info"],
    }


@router.post("/sentencetopayload")
async def sentence_to_payload(data: SentenceInput):
    sentence = data.sentence
    try:
        output = extract_payload(sentence)
        if output:
            # Ensure output is returned as a dictionary
            if hasattr(output, "model_dump"):
                return output.model_dump()
            elif isinstance(output, dict):
                return output

    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())
    except Exception as e:
        # Catch any other unexpected exceptions such as runtime errors, type errors, or unforeseen issues
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")




@router.get("/scrape_unpli_events")
async def scrape_unpli_events(page_no: int = 1, page_size: int = 10, session_id: str = "G1758362087062"):
    async with httpx.AsyncClient() as session:
        events = await scrape.fetch_unpli_events(session, page_no=page_no, page_size=page_size, session_id=session_id)
        if not events:
            raise HTTPException(status_code=404, detail="No events found or error fetching data.")
        transformed_events = await scrape.transform_events_for_json(events, session_id=session_id)

        # Save JSON locally (optional)
        save_dir = "./dataset"
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"veneto_unpliveneto_events_{page_no}_{page_size}.json")
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump({"events": transformed_events}, f, ensure_ascii=False, indent=4)

        return {"events": transformed_events, "saved_path": save_path}


@router.post("/processticketsqueezedelta")
async def process_ticketsqueeze_delta(
    file: UploadFile = File(...),
    include_removed: bool = False,
    include_changed: bool = True,
):
    """
    Process TicketSqueeze delta CSV file and transform to ingestible JSON events.
    
    Args:
        file: Delta CSV file from TicketSqueeze (with delta_type column)
        include_removed: Include removed events in output (default: False)
        include_changed: Include changed events in output (default: True)
    
    Returns:
        Dictionary with transformed events and processing summary
    """
    try:
        # Get the dataset folder path (remap/dataset, two levels up from backend/app/api)
        dataset_dir = Path(__file__).parent.parent.parent.parent / "dataset"
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Save uploaded file temporarily
        temp_csv_path = dataset_dir / f"temp_{file.filename}"
        
        with open(temp_csv_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Process delta CSV
        result = await ticketsqueeze.process_ticketsqueeze_daily_delta(
            delta_csv_path=Path(temp_csv_path),
            include_removed=include_removed,
            include_changed=include_changed
        )
        
        # Save processed events to JSON in the same dataset folder
        output_json_path = dataset_dir / f"ticketsqueeze_delta_{file.filename.replace('.csv', '.json')}"
        ticketsqueeze.save_events_to_json(result["events"], output_json_path)
        
        # Clean up temp CSV
        if temp_csv_path.exists():
            temp_csv_path.unlink()
        
        return {
            "status": "success",
            "events": result["events"],
            "summary": result["summary"],
            "saved_path": str(output_json_path)
        }
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error processing TicketSqueeze delta: {str(e)}")


@router.post("/ingestticketsqueezedelta")
async def ingest_ticketsqueeze_delta(file: UploadFile = File(...)):
    """
    Ingest TicketSqueeze delta JSON file directly into Qdrant without geocoding.
    Expects JSON with events that already have coordinates.
    
    Args:
        file: Pre-processed JSON file from /processticketsqueezedelta
    
    Returns:
        Dictionary with ingestion status and counts
    """
    if not file.filename.lower().endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are accepted")
    
    try:
        # Get the dataset folder path
        dataset_dir = Path(__file__).parent.parent.parent.parent / "dataset"
        dataset_dir.mkdir(parents=True, exist_ok=True)
        
        # Save uploaded file
        save_path = dataset_dir / file.filename
        
        with open(save_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # Load events directly (skip geocoding)
        logger.info(f"Loading TicketSqueeze events from {save_path}")
        with open(save_path, "r", encoding="utf-8") as f:
            events_data = json.load(f)
        
        events = events_data.get("events", [])
        logger.info(f"Loaded {len(events)} events from {file.filename}")
        
        # Ensure collection exists
        from app.services.ingest_service import ensure_collection_exists, calculate_hash, DENSE_VECTOR_NAME, SPARSE_VECTOR_NAME, COLLECTION_NAME
        from app.core.config import QDRANT_SERVER, QDRANT_API_KEY
        from qdrant_client import QdrantClient, models
        from uuid import uuid4
        
        ensure_collection_exists()
        
        client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=200000)
        
        # Normalize text function
        def normalize_text(text):
            if not text:
                return ""
            text = text.strip()
            import unicodedata
            text = unicodedata.normalize("NFKC", text)
            return text
        
        # Ingest without geocoding
        BATCH_SIZE = 32
        inserted = 0
        updated = 0
        skipped_unchanged = 0
        
        from tqdm import tqdm
        
        for start in tqdm(range(0, len(events), BATCH_SIZE)):
            batch = events[start: start + BATCH_SIZE]
            texts = [normalize_text(event.get("description", "")) for event in batch]
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
                    existing_hash = existing_point.payload.get("hash", None)
                    
                    if existing_hash == chunk_hash:
                        skipped_unchanged += 1
                        continue
                    else:
                        logger.info(f"Updating event ID: {event_id}")
                        point_id_to_use = existing_point.id
                        updated += 1
                else:
                    inserted += 1
                    point_id_to_use = str(uuid4())
                
                loc = event.get("location", {})
                loc_geo = {}
                if "latitude" in loc and "longitude" in loc:
                    loc_geo = {"lat": loc["latitude"], "lon": loc["longitude"]}
                
                location_payload = {**loc, **loc_geo}
                payload = {**event, "location": location_payload, "hash": chunk_hash}
                
                points.append(
                    models.PointStruct(
                        id=point_id_to_use,
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
            "status": "success",
            "filename": file.filename,
            "inserted": inserted,
            "updated": updated,
            "skipped_unchanged": skipped_unchanged,
            "collection_info": collection_info,
        }
    
    except Exception as e:
        logger.error(f"Error in ingest_ticketsqueeze_delta: {e}")
        raise HTTPException(status_code=400, detail=f"Error ingesting TicketSqueeze delta: {str(e)}")
