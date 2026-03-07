import logging
import os
from logging.handlers import TimedRotatingFileHandler
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.api.routes import router
from fastapi.middleware.cors import CORSMiddleware

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

app = FastAPI(default_response_class=ORJSONResponse)
app.include_router(router)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

logging.info(f"🚀 Logs → {os.path.abspath(log_file)}")
