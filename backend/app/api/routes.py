from fastapi import APIRouter, HTTPException, UploadFile, File
from app.services.ingest_service import ingest_events_from_file
from app.services.openroute_service import geocode_address, get_route
from app.services.qdrant_service import (
    build_geo_filter,
    build_date_intersection_filter,
    build_final_filter,
    query_events_hybrid,
)
from app.models import schemas
from app.models.schemas import SentenceInput
from app.services.extraction_service import extract_payload
from pydantic import ValidationError

from shapely.geometry import LineString, Point
import numpy as np
import geopandas as gpd
from qdrant_client.http import models as qmodels
from app.core.config import DENSE_MODEL_NAME, SPARSE_MODEL_NAME, COLLECTION_NAME
from fastembed import TextEmbedding, SparseTextEmbedding
import os
import shutil


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
