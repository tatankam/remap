import logging
from logging.handlers import TimedRotatingFileHandler
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.api.routes import router  # Import your routes module here
from fastapi.middleware.cors import CORSMiddleware

# --- Logging: weekly rotation, overwrite, BOTH console + file ---
handler = TimedRotatingFileHandler(
    "app.log", when="W0", interval=1, backupCount=0  # Overwrite every Monday
)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

# Configure root logger with BOTH handlers
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        handler,                    # File: app.log
        logging.StreamHandler()     # Console output
    ],
    force=True  # Critical: Override uvicorn's default config
)

# Force ALL loggers to use root handlers (fixes uvicorn override)
for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "httpx"):
    lg = logging.getLogger(logger_name)
    lg.handlers.clear()           # Remove uvicorn's duplicate handlers
    lg.propagate = True           # Send to root logger
    lg.setLevel(logging.INFO)

app = FastAPI(default_response_class=ORJSONResponse)  # Use ORJSON for faster JSON responses

# Include your API routes
app.include_router(router)

# CORS configuration
origins = [
    "*"  # You can specify frontend origins here if needed
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
