import logging
import os
from logging.handlers import TimedRotatingFileHandler
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.api.routes import router
from fastapi.middleware.cors import CORSMiddleware
from qdrant_client import QdrantClient
from app.core.config import QDRANT_SERVER, QDRANT_API_KEY, COLLECTION_NAME

# Log to ./backend/logs/app.log (works everywhere)
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "app.log")

handler = TimedRotatingFileHandler(log_file, when="W0", interval=1, backupCount=0)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler, logging.StreamHandler()],
    force=True
)

for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "httpx"):
    lg = logging.getLogger(name)
    lg.handlers.clear()
    lg.propagate = True
    lg.setLevel(logging.INFO)

# 🔥 LIFESPAN EVENTS - Qdrant warmup on startup
@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP: Warm Qdrant connection and indexes
    logging.info("🔥 Warming up Qdrant connection and indexes...")
    client = QdrantClient(url=QDRANT_SERVER, api_key=QDRANT_API_KEY)
    info = client.get_collection(COLLECTION_NAME)
    logging.info(f"Collection ready: {info.points_count} points")
    client.scroll(collection_name=COLLECTION_NAME, limit=1)  # Prime indexes into RAM
    logging.info("✅ Qdrant warmup complete")
    
    yield  # App runs here
    
    # SHUTDOWN (optional)
    logging.info("🛑 Application shutdown complete")

# Create app WITH lifespan parameter
app = FastAPI(
    default_response_class=ORJSONResponse, 
    lifespan=lifespan  # ← This fixes your 15s cold start
)

app.include_router(router)
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)

logging.info(f"🚀 Logs → {os.path.abspath(log_file)}")
