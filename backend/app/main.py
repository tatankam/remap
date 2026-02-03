import logging
import os
from logging.handlers import TimedRotatingFileHandler
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from app.api.routes import router
from fastapi.middleware.cors import CORSMiddleware

# Create log file path FIRST (don't configure yet)
log_file = os.path.join(os.getcwd(), "app.log")

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

# LOGGING CONFIG - AFTER app creation (critical for Docker)
def configure_logging():
    handler = TimedRotatingFileHandler(log_file, when="W0", interval=1, backupCount=0)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    
    # Root logger: file + console
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [handler, logging.StreamHandler()]
    
    # Redirect uvicorn loggers AFTER they exist
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "httpx"):
        lg = logging.getLogger(name)
        lg.handlers = []
        lg.propagate = True
        lg.setLevel(logging.INFO)

# Configure AFTER FastAPI setup
configure_logging()
logging.info("🚀 Logging configured - check /app/app.log")
