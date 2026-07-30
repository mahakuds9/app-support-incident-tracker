import os
from datetime import datetime
from typing import Optional
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from sqlmodel import SQLModel, Field, create_engine, Session, select
from prometheus_fastapi_instrumentator import Instrumentator

load_dotenv()  # reads .env into environment variables

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

app = FastAPI(title="Incident Tracker")
engine = create_engine(DATABASE_URL)

# Real request counts, real latencies, real status codes for every
# endpoint - exposed at /metrics in a format Prometheus scrapes.
Instrumentator().instrument(app).expose(app)


class Incident(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    title: str
    description: str
    severity: str  # P1, P2, P3
    status: str = "Open"  # Open, In Progress, Resolved
    created_at: datetime = Field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None


@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/incidents")
def create_incident(incident: Incident):
    with Session(engine) as session:
        session.add(incident)
        session.commit()
        session.refresh(incident)
        return incident


@app.get("/incidents")
def list_incidents():
    with Session(engine) as session:
        return session.exec(select(Incident)).all()


@app.get("/weather")
def get_weather(city: str):
    """
    Real dependency #2: this endpoint genuinely calls two external
    services (Open-Meteo's geocoding API, then its forecast API).
    No API key needed, but real network calls with real timeouts -
    if either call fails or times out, that's a genuine failure,
    not a simulated one.
    """
    try:
        geo_resp = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": city, "count": 1},
            timeout=5,
        )
        geo_resp.raise_for_status()
        geo_data = geo_resp.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Geocoding API failed: {e}")

    results = geo_data.get("results")
    if not results:
        raise HTTPException(status_code=404, detail=f"City '{city}' not found")

    lat = results[0]["latitude"]
    lon = results[0]["longitude"]
    resolved_name = results[0]["name"]

    try:
        weather_resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={"latitude": lat, "longitude": lon, "current_weather": True},
            timeout=5,
        )
        weather_resp.raise_for_status()
        weather_data = weather_resp.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Forecast API failed: {e}")

    return {
        "city": resolved_name,
        "latitude": lat,
        "longitude": lon,
        "current_weather": weather_data.get("current_weather"),
    }
