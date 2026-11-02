"""API du tableau de bord AquaSentinel : lit MongoDB, diffuse l'état en direct (SSE)
et publie les commandes d'injection de pannes vers Kafka (topic water.commands)."""
import asyncio
import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pymongo import MongoClient

from common.config import load_sectors

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
STALE_AFTER_S = 20          # un secteur sans nouvelle donnée depuis 20 s est affiché "hors ligne"
VALID_FAULTS = {"leak", "outage", "contamination", "repair"}

HERE = os.path.dirname(os.path.abspath(__file__))
SECTORS = load_sectors()
SECTOR_IDS = {s["id"] for s in SECTORS}

db = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=3000)["aquasentinel"]
_producer = None

app = FastAPI(title="AquaSentinel")
app.mount("/static", StaticFiles(directory=os.path.join(HERE, "static")), name="static")


def producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": BOOTSTRAP})
    return _producer


def iso(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def snapshot() -> dict:
    now = datetime.now(timezone.utc)
    states = {d["_id"]: d for d in db.sector_state.find()}

    sectors = []
    for s in SECTORS:
        item = {
            "id": s["id"], "name": s["name"], "lat": s["lat"], "lon": s["lon"],
            "population": s["population"],
            "nominal_pressure": s["nominal_pressure_bar"], "nominal_flow": s["nominal_flow_m3h"],
            "state": "UNKNOWN", "severity": 0, "message": "En attente de données",
            "pressure": None, "flow": None, "level": None, "turbidity": None,
            "est_loss": 0.0, "sim_hour": None, "age_s": None, "expected_flow": None,
        }
        st = states.get(s["id"])
        if st:
            age = (now - st["updated_at"]).total_seconds()
            item.update(
                state=st["state"] if age <= STALE_AFTER_S else "OFFLINE",
                severity=st["severity"], message=st["message"],
                pressure=st["pressure"], flow=st["flow"], level=st["level"],
                turbidity=st["turbidity"], est_loss=st["est_loss_m3h"],
                sim_hour=st["sim_hour"], age_s=round(age, 1),
                expected_flow=st.get("expected_flow"),
            )
        sectors.append(item)

    alerts = [{
        "id": str(a["_id"]), "ts": iso(a["ts"]), "sector_id": a["sector_id"], "name": a["name"],
        "type": a["type"], "severity": a["severity"], "kind": a["kind"], "message": a["message"],
    } for a in db.alerts.find().sort("ts", -1).limit(30)]

    ranking = [{
        "sector_id": r["sector_id"], "name": r["name"], "score": r["score"], "profile": r["profile"],
        "night_flow_ratio": r["night_flow_ratio"], "n_readings": r["n_readings"],
        "computed_at": iso(r["computed_at"]),
    } for r in db.sector_ranking.find().sort("score", -1)]

    return {"server_time": iso(now), "sectors": sectors, "alerts": alerts, "ranking": ranking}


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


@app.get("/api/health")
def health():
    try:
        db.command("ping")
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(503, f"MongoDB indisponible : {exc}")


@app.get("/api/snapshot")
def get_snapshot():
    return snapshot()


@app.get("/api/history/{sector_id}")
def history(sector_id: str, n: int = 60):
    if sector_id not in SECTOR_IDS:
        raise HTTPException(404, "secteur inconnu")
    docs = list(db.metrics.find({"sector_id": sector_id}).sort("ts", -1).limit(max(1, min(n, 300))))
    docs.reverse()
    return [{"ts": iso(d["ts"]), "pressure": d["pressure"], "flow": d["flow"],
             "level": d["level"], "turbidity": d["turbidity"], "state": d["state"]} for d in docs]


@app.get("/api/stream")
async def stream():
    async def events():
        while True:
            try:
                data = await asyncio.to_thread(snapshot)
                yield f"data: {json.dumps(data)}\n\n"
            except Exception as exc:
                yield f"event: problem\ndata: {json.dumps({'error': str(exc)})}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class Injection(BaseModel):
    sector_id: str
    fault: str


@app.post("/api/inject")
def inject(body: Injection):
    if body.fault not in VALID_FAULTS:
        raise HTTPException(400, f"panne invalide, choisir parmi {sorted(VALID_FAULTS)}")
    if body.sector_id != "*" and body.sector_id not in SECTOR_IDS:
        raise HTTPException(404, "secteur inconnu")
    msg = {"sector_id": body.sector_id, "fault": body.fault, "sent_at": time.time()}
    p = producer()
    p.produce("water.commands", key=body.sector_id, value=json.dumps(msg))
    p.flush(3)
    return {"sent": msg}
