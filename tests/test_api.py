"""Test de l'API du dashboard avec une fausse base (mongomock) et un faux producteur Kafka."""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

mongomock = pytest.importorskip("mongomock")
pytest.importorskip("fastapi")
pytest.importorskip("httpx")
import pymongo  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None):
        self.sent.append((topic, key, json.loads(value)))

    def flush(self, timeout=None):
        return 0


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(pymongo, "MongoClient", mongomock.MongoClient)
    monkeypatch.syspath_prepend(os.path.join(ROOT, "dashboard"))
    sys.modules.pop("app", None)
    import app as dash
    from fastapi.testclient import TestClient

    fake = FakeProducer()
    monkeypatch.setattr(dash, "_producer", fake)
    now = datetime.now(timezone.utc)
    dash.db.sector_state.insert_many([
        {"_id": "medina", "state": "LEAK", "severity": 2, "message": "Fuite probable", "est_loss_m3h": 210.0,
         "expected_flow": 400.0, "pressure": 2.4, "min_pressure": 2.3, "flow": 610.0, "level": 55.0,
         "turbidity": 0.6, "sim_hour": 9.5, "samples": 10, "updated_at": now},
        {"_id": "ariana", "state": "OK", "severity": 0, "message": "Fonctionnement nominal", "est_loss_m3h": 0.0,
         "expected_flow": 900.0, "pressure": 3.4, "min_pressure": 3.3, "flow": 905.0, "level": 72.0,
         "turbidity": 0.6, "sim_hour": 9.5, "samples": 10, "updated_at": now - timedelta(seconds=60)},  # périmé
    ])
    dash.db.alerts.insert_one({"sector_id": "medina", "name": "Médina de Tunis", "type": "LEAK", "severity": 2,
                               "kind": "open", "message": "Fuite probable", "ts": now})
    dash.db.metrics.insert_many([{"sector_id": "medina", "ts": now + timedelta(seconds=i), "state": "LEAK",
                                  "pressure": 2.4, "flow": 600 + i, "level": 55.0, "turbidity": 0.6}
                                 for i in range(5)])
    dash.db.sector_ranking.insert_one({"sector_id": "medina", "name": "Médina de Tunis", "score": 61.5,
                                       "profile": "Fragile", "night_flow_ratio": 1.8, "n_readings": 1234,
                                       "computed_at": now})
    return TestClient(dash.app), fake


def test_snapshot_merges_config_and_state(client):
    c, _ = client
    data = c.get("/api/snapshot").json()
    by_id = {s["id"]: s for s in data["sectors"]}
    assert len(by_id) == 10
    assert by_id["medina"]["state"] == "LEAK" and by_id["medina"]["expected_flow"] == 400.0
    assert by_id["ariana"]["state"] == "OFFLINE"          # pas de données depuis 60 s
    assert by_id["lac1"]["state"] == "UNKNOWN"            # jamais vu
    assert data["alerts"][0]["name"] == "Médina de Tunis"
    assert data["ranking"][0]["profile"] == "Fragile"


def test_history_is_chronological(client):
    c, _ = client
    flows = [p["flow"] for p in c.get("/api/history/medina").json()]
    assert flows == sorted(flows) and len(flows) == 5
    assert c.get("/api/history/inconnu").status_code == 404


def test_inject_publishes_command(client):
    c, fake = client
    assert c.post("/api/inject", json={"sector_id": "medina", "fault": "leak"}).status_code == 200
    assert c.post("/api/inject", json={"sector_id": "*", "fault": "repair"}).status_code == 200
    assert [(t, m["sector_id"], m["fault"]) for t, _, m in fake.sent] == [
        ("water.commands", "medina", "leak"), ("water.commands", "*", "repair")]
    assert c.post("/api/inject", json={"sector_id": "medina", "fault": "explosion"}).status_code == 400
    assert c.post("/api/inject", json={"sector_id": "nulle-part", "fault": "leak"}).status_code == 404


def test_index_is_served(client):
    c, _ = client
    r = c.get("/")
    assert r.status_code == 200 and "AquaSentinel" in r.text
