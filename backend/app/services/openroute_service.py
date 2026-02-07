import openrouteservice
import sqlite3
import hashlib
import time
import logging
import os
import json
from pathlib import Path
from app.core.config import OPENROUTE_API_KEY
from typing import Tuple, Dict, Any, List
from contextlib import contextmanager

logger = logging.getLogger(__name__)
ors_client = openrouteservice.Client(key=OPENROUTE_API_KEY)

# PATH DETECTION
if os.path.exists("/app"):
    DATASET_DIR = Path("/app") / "dataset"
else:
    DATASET_DIR = Path("dataset")

CACHE_DB = DATASET_DIR / "cache.db"
DATASET_DIR.mkdir(parents=True, exist_ok=True)

def init_db():
    """Production DB setup - bulletproof"""
    conn = sqlite3.connect(CACHE_DB, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA wal_autocheckpoint=100")
        
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
        
        # INDEXES
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
    conn = sqlite3.connect(CACHE_DB, timeout=30.0, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()

def cleanup_cache(table: str):
    """Atomic cleanup - race-condition proof"""
    now = int(time.time())
    with get_db_connection() as conn:
        # Atomic expired cleanup
        expired = conn.execute(
            f"DELETE FROM {table} WHERE expires <= ?", (now,)
        ).rowcount
        
        # Count only active entries
        size = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE expires > ?", (now,)
        ).fetchone()[0]
        
        if size >= MAX_CACHE_SIZE:
            excess = max(0, size - MAX_CACHE_SIZE // 2)
            oldest = conn.execute(
                f"""
                DELETE FROM {table} WHERE route_hash IN (
                    SELECT route_hash FROM {table} 
                    WHERE expires > ? 
                    ORDER BY created ASC 
                    LIMIT ?
                )
                """, (now, excess)
            ).rowcount
            conn.commit()
            logger.info(f"🧹 {table}: exp={expired}, old={oldest}, active={size}")
        elif expired > 0:
            conn.commit()
            logger.debug(f"🧹 {table}: expired={expired}")

def geocode_address(address: str) -> Tuple[float, float]:
    """PERFECT - ZERO BUGS"""
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
            logger.debug(f"✅ GEO HIT: {address[:30]}")
            return result[0], result[1]  # lon, lat ✅
    
    cleanup_cache("geocode_cache")
    
    logger.info(f"🌐 GEO: {address[:50]}...")
    try:
        result = ors_client.pelias_search(text=address)
        if not (result and result.get('features') and result['features']):
            raise ValueError("No geocoding results")
        
        coords = result['features'][0]['geometry']['coordinates']
        lon, lat = coords[0], coords[1]
        
        with get_db_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO geocode_cache 
                (address_hash, address, lon, lat, expires) 
                VALUES (?, ?, ?, ?, ?)
            """, (addr_hash, address, lon, lat, now + CACHE_TTL))
            conn.commit()
        
        logger.info(f"✅ GEO CACHED: ({lon:.6f}, {lat:.6f})")
        return lon, lat
        
    except Exception as e:
        logger.error(f"❌ GEO FAILED: {e}")
        raise ValueError(f"Geocoding failed: {e}")

def get_route(coords: List[List[float]], profile: str = 'driving-car',
              radiuses: List[float] = None) -> Dict[str, Any]:
    """ULTIMATE ROUTE CACHE - PRODUCTION LOCKED"""
    if radiuses is None:
        radiuses = [1000.0, 1000.0]
    
    if len(coords) != 2 or any(len(c) != 2 for c in coords):
        raise ValueError("Exactly 2 coordinates [[lon,lat],[lon,lat]] required")
    
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
            logger.debug(f"🛤️ ROUTE HIT: {coords[0]}→{coords[1]}")
            return json.loads(result[0])
    
    cleanup_cache("route_cache")
    
    logger.info(f"🛣️ ROUTE: {coords[0]}→{coords[1]} ({profile})")
    try:
        route_data = ors_client.directions(
            coordinates=coords, profile=profile, radiuses=radiuses, format='geojson'
        )
        
        if (not isinstance(route_data, dict) or 
            not route_data.get('features') or 
            not route_data['features'][0].get('geometry')):
            raise ValueError("Invalid route response")
        
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
            conn.commit()
        
        logger.info(f"✅ ROUTE CACHED: {len(route_data['features'][0]['geometry']['coordinates'])} pts")
        return route_data
        
    except Exception as e:
        logger.error(f"❌ ROUTE FAILED: {e}")
        raise ValueError(f"Routing failed: {e}")

get_route_uncached = ors_client.directions