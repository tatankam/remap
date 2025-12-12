from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="../.env")

QDRANT_SERVER = os.getenv("QDRANT_SERVER")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")


OPENROUTE_API_KEY = os.getenv("OPENROUTE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPEN_AI_BASE_URL = os.getenv("OPEN_AI_BASE_URL")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")
# Add dense and sparse model names to config
DENSE_MODEL_NAME = os.getenv("DENSE_MODEL_NAME")
SPARSE_MODEL_NAME = os.getenv("SPARSE_MODEL_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
#COLLECTION_NAME = "veneto_events"
#COLLECTION_NAME = "veneto_unpliveneto_events"
UNPLI_SESSION_ID = os.getenv("UNPLI_SESSION_ID")
