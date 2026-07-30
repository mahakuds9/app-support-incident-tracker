# App Support Incident Tracker

A live, self-hosted monitoring and incident tracking system built to
practice real Application Support skills — genuine monitoring of a
genuine running application, not simulated or fabricated data.

## Why this project is different

Most portfolio "incident tracker" projects fake their data — a script
randomly generates fictional outages on a timer. This project
deliberately avoids that. Every incident logged here comes from a
real, scheduled health check that actually detected a real problem
(a real timeout, a real non-200 response, a real slow endpoint).
If nothing is genuinely wrong, nothing gets logged — an empty
incident list is a correct, honest result, not a missing feature.

## Architecture

- **FastAPI** — the core application. Exposes:
  - `GET /health` — basic liveness check
  - `POST /incidents` / `GET /incidents` — real incident records
  - `GET /weather?city=...` — a real feature that depends on two
    live external calls (Open-Meteo geocoding + forecast APIs),
    giving the app a genuine external failure surface
  - `GET /metrics` — real request counts and latencies, exposed via
    `prometheus-fastapi-instrumentator`

- **PostgreSQL** (Docker, persistent volume) — the app's real
  database. Replaced an earlier SQLite prototype.

- **Apache Airflow** (Docker, `LocalExecutor`) — real scheduled
  monitoring, standing in for the conceptual role Control-M plays
  in enterprise environments. A DAG (`app_monitor_dag`) runs every
  2 minutes, genuinely calls `/health` and `/weather`, and only
  creates an incident (via a real `POST /incidents` call) if a
  real failure or real threshold breach is detected. Uses its own
  separate metadata database, matching real practice of not mixing
  orchestration data with application data.

- **Prometheus** (Docker) — scrapes the app's real `/metrics`
  endpoint every 15 seconds, storing real time-series data.

- **Grafana** (Docker) — visualizes that data on a live dashboard
  (`Incident Tracker - App Health`), currently with two panels:
  average response time by endpoint, and request rate by endpoint.

## Local setup

Prerequisites: Docker, Python 3.12, a virtual environment.

```bash
# 1. Start the infrastructure (Postgres x2, Airflow, Prometheus, Grafana)
docker compose up -d

# 2. Install app dependencies
pip install -r requirements.txt

# 3. Run the app (0.0.0.0 so Docker containers can reach it)
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Services once running:
| Service | URL |
|---|---|
| App | http://localhost:8000 |
| Airflow | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## Status

Working end-to-end and manually verified:
- Real Postgres persistence
- Real external dependency (`/weather`)
- Real scheduled monitoring via Airflow, confirmed to only log
  genuine incidents
- Real metrics scraping and visualization via Prometheus + Grafana

Not yet done:
- Public deployment (Render + custom domain via GoDaddy)
- Additional Grafana panels (e.g. error rate)
