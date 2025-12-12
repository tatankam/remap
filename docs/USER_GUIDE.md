# 🗺️ User Guide

## 📘 Introduction

Welcome to the **ReMap** project! This application allows you to visualize events along a route using combined backend and frontend services. The backend provides required APIs, and a Streamlit UI enables interactive route and event visualization.

## 💻 System Requirements

- Docker and Docker Compose installed (for easy deployment), or  
- Python 3.13+ if running locally without Docker.  
- Required API keys configured as environment variables.

## 🔐 Required API Keys (Environment Variables)

To run **ReMap** you must configure the following environment variables:

- `QDRANT_SERVER`  
- `QDRANT_API_KEY`  
- `OPENROUTE_API_KEY`  
- `OPENAI_API_KEY`  
- `OPEN_AI_BASE_URL`  
- `OPENAI_MODEL`  

Ensure these keys are securely stored and accessible to services at runtime.

## ⚙️ Installation and Setup

### 📥 Clone Repository (for all setups)

1. Clone the repository:

    ```
    git clone <repo-url>
    cd <repo-folder>
    ```

---
### 📝 Environment Configuration

Before running the application, copy the example environment file and fill in your own credentials:

```
cp .env.example .env
```

Open `.env` in a text editor and provide the required API keys and endpoints as described above.

---


### 🐳 Docker Setup

1. Build and run all services:

    ```
    docker compose up --build
    ```

---

### 🖥️ Local Setup

1. Create and activate a Python virtual environment.

2. Install backend dependencies:

    ```
    pip install -r backend/requirements.txt
    ```

3. Install frontend dependencies:

    ```
    pip install -r frontend/requirements.txt
    ```

4. Start services separately from their folders:

    - Backend (from `remap/backend`):

      ```
      uvicorn app.main:app --reload
      ```

    - Frontend (from `remap/frontend`):

      ```
      streamlit run streamlit_app.py
      ```

---

## 🌐 Accessing the Application

- Frontend UI: [http://localhost:8501](http://localhost:8501)  
- Backend API docs: [http://localhost:8000/docs](http://localhost:8000/docs)

---

# 🚀 Getting Started

- Use manual or natural language input modes to query events along travel routes.  
- Upload event data via provided tools.  
- Explore interactive map with event markers and filters.  
- Review troubleshooting tips if issues arise.

### 🧭 User Interface Overview

- Input addresses and travel profile manually or input natural language travel plans.

#### ✍️ Manual Input

![Input manually](./images/manualinput.png "Enter the input data")

- Specify buffer distance, date ranges, and query text to filter events.

- Events are displayed interactively on the map along the travel route.

![Manual Output](./images/manualoutput.png "Events Displayed on Map")

#### 💬 Natural Language Input

![Natural Language Input](./images/naturallanguageinput.png "Natural Language Input Mode")

- Specify buffer distance, date ranges, and query text as natural language sentence.

![Natural Language Output](./images/naturallanguageoutput.png "Events Displayed on Map")

---

## 🔧 Core Features Usage

### 🗺️ Creating an Event Map

1. Enter origin and destination addresses or write a natural language sentence describing your route.
2. Set buffer radius (in km) to specify the event search area.
3. Choose transport profile (car, bike, walking).
4. Set date and time ranges to filter event schedules.
5. Submit and explore generated events on the interactive map.

### 🧠 Natural Language Query

Use the frontend's natural language input mode to describe travel plans naturally, like:

> "I want to go from Vicenza to Trento leaving 2 September 2025 at 2 a.m., arriving 18 October 2025 at 5 a.m., and show me 10 music events within 6 km using bike."

### 📂 Uploading and Managing Event Data

All event datasets are maintained in the `dataset/` directory and initially prepared using the Jupyter notebooks located in the `notebooks/` folder. To add new events, create JSON files that adhere to the structure defined in the provided template: `dataset/veneto_events_template.json`. 

These JSON files can then be uploaded to the system via the backend `/ingestevents` API endpoint. Upon upload, events are processed and indexed in the Qdrant vector database, enabling efficient and fast retrieval during route-based searches and queries.

UPDATE October 2025
New service scraping scrape.py
It scrapes veneto sagre and it creates a \backend\dataset\veneto_unpliveneto_events_1_500.json
See script ingesting.sh

### 📂 Uploading and Managing Event Data of ticketsqueeze

### 📥 Downloading the daily TicketSqueeze CSV

To ingest the latest events, use the `ingestticketsqueeze.sh` script, which downloads the daily CSV from the TicketSqueeze FTP and manages the local history.

1. Configure the FTP credentials in a `.env` file (same folder as the script):


2. Run the ingestion script from the project root (it writes into `./remap/dataset` and handles rotation/cleanup):


cd ./remap

./ingestticketsqueeze.sh



To create the json from the csv:
curl -X POST "http://127.0.0.1:8000/processticketsqueezedelta"   -F "file=@/home/biso/development/my_projects/remap/dataset/delta.csv"   -F "include_removed=false"   -F "include_changed=false"


To load the json:

curl -X POST "http://127.0.0.1:8000/ingestticketsqueezedelta"   -F "file=@/home/biso/development/my_projects/remap/dataset/ticketsqueeze_delta_delta.json"

---

## 🛠️ Troubleshooting

- Ensure Docker is running and required ports (8000, 8501) are free.  
- Verify environment variables are correctly set.  
- Check backend logs for errors.  
- Clear browser cache as needed.


## 📬 Contact and Support

For issues or questions, please open an issue on the GitHub repository.

---

Thank you for using **ReMap**! Happy mapping and discovery!