import openrouteservice
import requests
import sqlite3
import hashlib
import time
import logging
import os
import json
from pathlib import Path
from app.core.config import OPENROUTE_API_KEY
from app.core import config
from typing import Tuple, Dict, Any, List, Optional
from contextlib import contextmanager
import atexit

logger = logging.getLogger(__name__)
ors_client = openrouteservice.Client(key=OPENROUTE_API_KEY)

# GLOBAL HTTP SESSION
_photon_session = requests.Session()
_photon_session.headers.update({
    'User-Agent': f'{config.PHOTON_USER_AGENT} ({config.PHOTON_CONTACT_EMAIL})',
    'Accept': 'application/json',
})

# PATH DETECTION
if os.path.exists("/app"):
    DATASET_DIR = Path("/app") / "dataset"
else:
    DATASET_DIR = Path("dataset")

CACHE_DB = DATASET_DIR / "cache.db"
DATASET_DIR.mkdir(parents=True, exist_ok=True)

def init_db():
    """Production DB setup"""
    conn = sqlite3.connect(CACHE_DB, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA wal_autocheckpoint=100")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA optimize")
        
        # GEOCODE CACHE
        conn.execute("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                address_hash TEXT PRIMARY KEY,
                address TEXT NOT NULL,
                lon REAL NOT NULL,
                lat REAL NOT NULL,
                expires INTEGER NOT NULL,
                created INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
        """)
        
        # ROUTE CACHE
        conn.execute("""
            CREATE TABLE IF NOT EXISTS route_cache (
                route_hash TEXT PRIMARY KEY,
                start_lon REAL NOT NULL,
                start_lat REAL NOT NULL,
                end_lon REAL NOT NULL,
                end_lat REAL NOT NULL,
                profile TEXT NOT NULL,
                route_json TEXT NOT NULL,
                expires INTEGER NOT NULL,
                created INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
        """)
        
        conn.execute("CREATE INDEX IF NOT EXISTS idx_g_expires ON geocode_cache(expires)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_r_expires ON route_cache(expires)")
        conn.commit()
        logger.info(f"✅ DB READY: {CACHE_DB.absolute()}")
    finally:
        conn.close()

init_db()

CACHE_TTL = 90 * 86400
MAX_CACHE_SIZE = 20000

@contextmanager
def get_db_connection():
    """Thread-safe DB connection"""
    conn = sqlite3.connect(CACHE_DB, timeout=30.0, check_same_thread=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def cleanup_cache(table: str):
    """Atomic cleanup"""
    now = int(time.time())
    with get_db_connection() as conn:
        expired = conn.execute(
            f"DELETE FROM {table} WHERE expires <= ?", (now,)
        ).rowcount
        
        size = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE expires > ?", (now,)
        ).fetchone()[0]
        
        if size >= MAX_CACHE_SIZE:
            excess = max(0, size - MAX_CACHE_SIZE // 2)
            # Use appropriate hash column based on table
            hash_col = "address_hash" if table == "geocode_cache" else "route_hash"
            conn.execute(
                f"""
                DELETE FROM {table} WHERE {hash_col} IN (
                    SELECT {hash_col} FROM {table} 
                    WHERE expires > ? 
                    ORDER BY created ASC 
                    LIMIT ?
                )
                """, (now, excess)
            )
            logger.info(f"🧹 {table} resized: active={size}")
        elif expired > 0:
            logger.debug(f"🧹 {table}: expired={expired}")

def photon_geocode(address: str) -> Optional[Tuple[float, float]]:
    """Photon geocoding with connection pooling"""
    try:
        params = {
            'q': address,
            'limit': '1',
            'osm_tag': ['place:city', 'place:town', 'place:village'],
        }
        response = _photon_session.get(config.PHOTON_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if data.get('features'):
            feature = data['features'][0]
            coords = feature['geometry']['coordinates']
            return coords[0], coords[1]
        
        return None
    except Exception as e:
        logger.debug(f"Photon failed: {e}")
        return None

def geocode_address(address: str) -> Tuple[float, float]:
    """Photon → ORS fallback - FULLY CACHED"""
    if len(address := address.strip()) < 3:
        raise ValueError("Address too short")
    
    addr_hash = hashlib.md5(address.lower().encode()).hexdigest()
    now = int(time.time())
    
    with get_db_connection() as conn:
        result = conn.execute(
            "SELECT lon, lat FROM geocode_cache WHERE address_hash=? AND expires > ?",
            (addr_hash, now)
        ).fetchone()
        
        if result:
            return result[0], result[1]
    
    cleanup_cache("geocode_cache")
    
    # 1. Photon
    res = photon_geocode(address)
    lon, lat = res if res else (None, None)
    source = "Photon" if lon else "Empty"
    
    # 2. ORS fallback
    if lon is None:
        try:
            result = ors_client.pelias_search(text=address)
            if result and result.get('features'):
                coords = result['features'][0]['geometry']['coordinates']
                lon, lat = coords[0], coords[1]
                source = "ORS"
            else:
                raise ValueError("No geocoding results")
        except Exception as e:
            logger.error(f"❌ Geocoding failed: {e}")
            raise ValueError(f"Geocoding failed: {e}")
    
    # Cache success
    with get_db_connection() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO geocode_cache 
            (address_hash, address, lon, lat, expires) 
            VALUES (?, ?, ?, ?, ?)
        """, (addr_hash, address, lon, lat, now + CACHE_TTL))
    
    logger.info(f"✅ GEO CACHED ({source}): ({lon:.6f}, {lat:.6f})")
    return lon, lat

def get_route(coords: List[List[float]], profile: str = 'driving-car',
              radiuses: List[float] = None) -> Dict[str, Any]:
    """ULTIMATE ROUTE CACHE - Supports Route Mode with GeoJSON validation"""
    if radiuses is None:
        radiuses = [1000.0, 1000.0]
    
    if len(coords) != 2:
        raise ValueError("Exactly 2 coordinates [[lon,lat],[lon,lat]] required for route")
    
    coords_key = json.dumps(coords, separators=(',', ':'))
    radiuses_key = json.dumps(radiuses, separators=(',', ':'))
    route_hash = hashlib.md5(f"{coords_key}:{profile}:{radiuses_key}".encode()).hexdigest()
    now = int(time.time())
    
    with get_db_connection() as conn:
        result = conn.execute(
            "SELECT route_json FROM route_cache WHERE route_hash=? AND expires > ?",
            (route_hash, now)
        ).fetchone()
        
        if result:
            return json.loads(result[0])
    
    cleanup_cache("route_cache")
    
    try:
        # Requesting format='geojson' returns a FeatureCollection
        route_data = ors_client.directions(
            coordinates=coords, 
            profile=profile, 
            radiuses=radiuses, 
            format='geojson'
        )
        
        # ✅ FIX: Validate for GeoJSON FeatureCollection structure
        if ('features' not in route_data or 
            not route_data['features'] or 
            'geometry' not in route_data['features'][0]):
            raise ValueError("Invalid GeoJSON route response")
        
        with get_db_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO route_cache 
                (route_hash, start_lon, start_lat, end_lon, end_lat, profile, route_json, expires)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                route_hash, coords[0][0], coords[0][1],
                coords[1][0], coords[1][1], profile,
                json.dumps(route_data), now + CACHE_TTL
            ))
        
        pts_count = len(route_data['features'][0]['geometry']['coordinates'])
        logger.info(f"✅ ROUTE CACHED: {pts_count} pts")
        return route_data
        
    except Exception as e:
        logger.error(f"❌ ROUTE FAILED: {e}")
        raise ValueError(f"Routing failed: {e}")

# Cleanup on shutdown
def cleanup_session():
    _photon_session.close()

atexit.register(cleanup_session)

# Allow raw client access if needed
get_route_uncached = ors_client.directions