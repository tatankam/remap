from dotenv import load_dotenv
import os

# Carica il file .env dalla root del progetto
load_dotenv(dotenv_path="../.env")

# --- QDRANT CONFIGURATION ---
QDRANT_SERVER = os.getenv("QDRANT_SERVER")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# --- AI & EMBEDDING MODELS ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPEN_AI_BASE_URL = os.getenv("OPEN_AI_BASE_URL")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")
DENSE_MODEL_NAME = os.getenv("DENSE_MODEL_NAME")
SPARSE_MODEL_NAME = os.getenv("SPARSE_MODEL_NAME")

# --- GEO SERVICES ---
OPENROUTE_API_KEY = os.getenv("OPENROUTE_API_KEY")
PHOTON_BASE_URL = os.getenv("PHOTON_BASE_URL")
PHOTON_USER_AGENT = os.getenv("PHOTON_USER_AGENT") 
PHOTON_CONTACT_EMAIL = os.getenv("PHOTON_CONTACT_EMAIL")

# --- UNPLI / DMS VENETO ---
UNPLI_SESSION_ID = os.getenv("UNPLI_SESSION_ID")
UNPLI_API_BASE_URL = os.getenv("UNPLI_API_BASE_URL")
UNPLI_WEB_BASE_URL = os.getenv("UNPLI_WEB_BASE_URL")

# --- TICKETMASTER & AFFILIATE ---
# Legge il link di Impact direttamente dal .env senza default nel codice
IMPACT_BASE_URL = os.getenv("IMPACT_BASE_URL")

# Legge il prefisso PROVIDER_ID (es. TM) dal .env
TM_PROVIDER_PREFIX = os.getenv("TM_PROVIDER_PREFIX")
IMPACT_MEMBER_ID = os.getenv("IMPACT_MEMBER_ID")
# Endpoint opzionale
TICKETMASTER_API_BASE_URL = os.getenv("TICKETMASTER_API_BASE_URL")
UNPLI_PROVIDER_PREFIX = os.getenv("UNPLI_PROVIDER_PREFIX")