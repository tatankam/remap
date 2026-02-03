import logging
import os
from logging.handlers import TimedRotatingFileHandler
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.api.routes import router
from fastapi.middleware.cors import CORSMiddleware

# --- FORCE /app/app.log (Docker) + ./app.log (Local) ---
app_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of main.py
log_file = os.path.join(app_dir, "app.log")          # app/app.log in Docker

handler = TimedRotatingFileHandler(
    log_file, when="W0", interval=1, backupCount=0
)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler, logging.StreamHandler()],
    force=True
)

for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "httpx"):
    lg = logging.getLogger(logger_name)
    lg.handlers.clear()
    lg.propagate = True
    lg.setLevel(logging.INFO)

app = FastAPI(default_response_class=ORJSONResponse)
app.include_router(router)

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Test log
logging.info(f"🚀 Logging to: {log_file}")
