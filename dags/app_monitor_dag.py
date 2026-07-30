"""
app_monitor_dag.py

Real, scheduled health-checking - the Airflow equivalent of what
Control-M would do for scheduled monitoring jobs in production.

Runs every 2 minutes. Calls the real /health and /weather endpoints
on the live FastAPI app. Only creates an Incident if something
genuinely failed or was genuinely slow - nothing here is simulated.
"""

import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

APP_BASE_URL = os.getenv("APP_BASE_URL", "http://host.docker.internal:8000")

# Real thresholds - not arbitrary. A health check should be near-instant;
# anything over 2s is genuinely slow for a single DB-backed endpoint.
HEALTH_SLOW_THRESHOLD_SECONDS = 2.0
# /weather makes two real external calls, so it naturally takes longer -
# 5s is a fair real threshold before calling it "degraded."
WEATHER_SLOW_THRESHOLD_SECONDS = 5.0

REQUEST_TIMEOUT_SECONDS = 8  # if it takes longer than this, treat as down


def _create_incident(title, description, severity, logs=""):
    """Logs a real incident via the app's own /incidents endpoint."""
    try:
        requests.post(
            f"{APP_BASE_URL}/incidents",
            json={
                "title": title,
                "description": f"{description} {logs}".strip(),
                "severity": severity,
            },
            timeout=5,
        )
    except requests.exceptions.RequestException as e:
        # If we can't even reach the app to log the incident, that itself
        # is worth knowing - surface it in the task logs, don't hide it.
        print(f"Failed to write incident to app: {e}")


def check_health(**context):
    start = datetime.utcnow()
    try:
        resp = requests.get(f"{APP_BASE_URL}/health", timeout=REQUEST_TIMEOUT_SECONDS)
        elapsed = (datetime.utcnow() - start).total_seconds()

        if resp.status_code != 200:
            _create_incident(
                title="Health check failed",
                description=f"/health returned status {resp.status_code}.",
                severity="P1",
                logs=f"Response: {resp.text[:200]}",
            )
        elif elapsed > HEALTH_SLOW_THRESHOLD_SECONDS:
            _create_incident(
                title="Slow health check response",
                description=f"/health took {elapsed:.2f}s, threshold is {HEALTH_SLOW_THRESHOLD_SECONDS}s.",
                severity="P3",
            )
        else:
            print(f"Health check OK ({elapsed:.2f}s)")

    except requests.exceptions.RequestException as e:
        _create_incident(
            title="App unreachable",
            description="Health check could not connect to the app at all.",
            severity="P1",
            logs=str(e),
        )


def check_weather_dependency(**context):
    start = datetime.utcnow()
    try:
        resp = requests.get(
            f"{APP_BASE_URL}/weather",
            params={"city": "Bengaluru"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        elapsed = (datetime.utcnow() - start).total_seconds()

        if resp.status_code != 200:
            _create_incident(
                title="Weather dependency check failed",
                description=f"/weather returned status {resp.status_code}.",
                severity="P2",
                logs=f"Response: {resp.text[:200]}",
            )
        elif elapsed > WEATHER_SLOW_THRESHOLD_SECONDS:
            _create_incident(
                title="Slow external API dependency",
                description=f"/weather took {elapsed:.2f}s, threshold is {WEATHER_SLOW_THRESHOLD_SECONDS}s.",
                severity="P3",
            )
        else:
            print(f"Weather dependency OK ({elapsed:.2f}s)")

    except requests.exceptions.RequestException as e:
        _create_incident(
            title="Weather dependency unreachable",
            description="Could not reach /weather endpoint at all.",
            severity="P2",
            logs=str(e),
        )


default_args = {
    "owner": "saroj",
    "retries": 0,
}

with DAG(
    dag_id="app_monitor_dag",
    default_args=default_args,
    description="Real, scheduled monitoring of the live Incident Tracker app",
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2026, 7, 29),
    catchup=False,
    tags=["monitoring", "app-support"],
) as dag:

    health_task = PythonOperator(
        task_id="check_health",
        python_callable=check_health,
    )

    weather_task = PythonOperator(
        task_id="check_weather_dependency",
        python_callable=check_weather_dependency,
    )

    health_task >> weather_task
