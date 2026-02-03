import logging
import os
from logging.handlers import TimedRotatingFileHandler
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.api.routes import router
from fastapi.middleware.cors import CORSMiddleware

# --- Logging: weekly rotation, overwrite, BOTH console + /app/app.log ---
log_file = os.path.join(os.getcwd(), "app.log")  # Ensures /app/app.log in Docker
handler = TimedRotatingFileHandler(
    log_file, when="W0", interval=1, backupCount=0  # Overwrite every Monday
)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

# Configure root logger with BOTH handlers
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        handler,                    # File: /app/app.log
        logging.StreamHandler()     # Console → docker logs
    ],
    force=True  # Override uvicorn's config
)

# Force ALL loggers to use root handlers
for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "httpx"):
    lg = logging.getLogger(logger_name)
    lg.handlers.clear()
    lg.propagate = True
    lg.setLevel(logging.INFO)

app = FastAPI(default_response_class=ORJSONResponse)

# Include your API routes
app.include_router(router)

# CORS configuration
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
