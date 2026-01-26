import openrouteservice
import sqlite3
import hashlib
import time
import logging
import os
from pathlib import Path
from app.core.config import OPENROUTE_API_KEY
from typing import Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)
ors_client = openrouteservice.Client(key=OPENROUTE_API_KEY)

# ✅ SMART PATH: Detects Docker vs Local automatically
if os.path.exists("/app"):
    # Docker environment
    DATASET_DIR = Path("/app") / "dataset"
else:
    # Local development  
    DATASET_DIR = Path("dataset")

CACHE_DB = DATASET_DIR / "geocode_cache.db"

# Ensure dataset directory exists
DATASET_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"✅ Cache DB: {CACHE_DB.absolute()} | Docker: {os.path.exists('/app')}")

# Config
CACHE_TTL = 90 * 86400
MAX_CACHE_SIZE = 20000

@contextmanager
def get_db_connection():
    """Per-request connection - fixes FastAPI locking"""
    conn = sqlite3.connect(CACHE_DB, timeout=30.0, check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()

def geocode_address(address: str) -> Tuple[float, float]:
    """95% faster geocoding with SQLite FIFO cache - FIXED FOR FASTAPI"""
    if not address or len(address.strip()) < 3:
        raise ValueError(f"Address too short: {address}")
    
    address = address.strip()
    addr_hash = hashlib.md5(address.lower().encode()).hexdigest()
    now = int(time.time())
    
    # 1. CHECK CACHE
    with get_db_connection() as conn:
        result = conn.execute(
            "SELECT lat, lon FROM geocode_cache WHERE address_hash=? AND expires > ?",
            (addr_hash, now)
        ).fetchone()
        
        if result:
            logger.debug(f"✅ CACHE HIT: {address[:50]}...")
            return (result[1], result[0])  # lon, lat for ORS
    
    # 2. FIFO CLEANUP if full
    with get_db_connection() as conn:
        current_size = conn.execute(
            "SELECT COUNT(*) FROM geocode_cache WHERE expires > ?", (now,)
        ).fetchone()[0]
        
        if current_size >= MAX_CACHE_SIZE:
            deleted = conn.execute("""
                DELETE FROM geocode_cache 
                WHERE address_hash IN (
                    SELECT address_hash FROM geocode_cache 
                    WHERE expires > ? 
                    ORDER BY expires ASC 
                    LIMIT ?
                )
            """, (now, current_size - MAX_CACHE_SIZE // 2)).rowcount
            conn.commit()
            if deleted > 0:
                logger.info(f"🧹 FIFO: deleted {deleted}")
    
    # 3. ORS API CALL + CACHE
    logger.info(f"🌐 ORS: {address[:50]}...")
    try:
        geocode_result = ors_client.pelias_search(text=address)
        if (geocode_result and 'features' in geocode_result and 
            len(geocode_result['features']) > 0):
            coords = geocode_result['features'][0]['geometry']['coordinates']
            
            # Cache result
            with get_db_connection() as conn:
                expires = now + CACHE_TTL
                conn.execute(
                    "INSERT OR REPLACE INTO geocode_cache VALUES (?, ?, ?, ?)",
                    (addr_hash, coords[1], coords[0], expires)
                )
                conn.commit()
            
            logger.info(f"✅ NEW CACHE: {address[:50]}...")
            return tuple(coords)
        else:
            raise ValueError(f"No results: {address}")
    except Exception as e:
        logger.error(f"❌ ORS FAILED: {address[:50]}... {e}")
        raise ValueError(f"Cannot geocode: {address}")

def get_route(coords, profile, radiuses=[1000, 1000]):
    """Original routing - unchanged"""
    return ors_client.directions(
        coordinates=coords, 
        profile=profile, 
        radiuses=radiuses, 
        format='geojson'
    )
