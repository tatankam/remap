from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from app.core.config import QDRANT_SERVER, QDRANT_API_KEY, COLLECTION_NAME


qdrant_client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY, timeout=4000000)

def build_geo_filter(polygon_coords_qdrant):
    return qmodels.Filter(
        must=[
            qmodels.FieldCondition(
                key="location",
                geo_polygon=qmodels.GeoPolygon(
                    exterior=qmodels.GeoLineString(points=polygon_coords_qdrant)
                )
            )
        ]
    )

def build_date_intersection_filter(start_date, end_date):
    return qmodels.Filter(
        must=[
            qmodels.FieldCondition(
                key="start_date",
                range=qmodels.DatetimeRange(lte=end_date)
            ),
            qmodels.FieldCondition(
                key="end_date",
                range=qmodels.DatetimeRange(gte=start_date)
            )
        ]
    )

def build_final_filter(geo_filter, date_filter):
    return qmodels.Filter(must=geo_filter.must + date_filter.must)

def query_events_hybrid(dense_vector, sparse_vector, query_filter, collection_name=COLLECTION_NAME, limit=100, score_threshold=0.0):
    results = qdrant_client.query_points(
        collection_name=collection_name,
        prefetch=[
            qmodels.Prefetch(
                query=qmodels.SparseVector(
                    indices=list(sparse_vector.indices),
                    values=list(sparse_vector.values)
                ),
                using="sparse_vector",
                limit=50,
                # score_threshold=score_threshold,  # Optional: filter out low-score results but I don't need for sparse
            ),
            qmodels.Prefetch(
                query=dense_vector,
                using="dense_vector",
                limit=50,
                score_threshold=score_threshold,  # Optional: filter out low-score results
            ),
        ],
        query=qmodels.FusionQuery(fusion=qmodels.Fusion.RRF),
        query_filter=query_filter,
        limit=limit,
        with_payload=True,
        # score_threshold=score_threshold,  # Optional: filter out low-score results
    )

    # Process results into dataframe
    records = []
    for point in results.points:
        entry = dict(point.payload)
        entry["score"] = point.score
        records.append(entry)
    
    #return [p.payload for p in results.points]
    return records
#    return [{"payload": p.payload, "score": p.score} for p in results.points]

