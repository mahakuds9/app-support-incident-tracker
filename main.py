from fastapi import FastAPI

app = FastAPI(title="Incident Tracker")


@app.get("/health")
def health():
    return {"status": "ok"}
