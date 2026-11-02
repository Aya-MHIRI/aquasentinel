"""Vérifie que le simulateur et les règles de détection sont cohérents entre eux :
aucune fausse alerte en fonctionnement normal, détection de chaque type de panne,
et retour à la normale après réparation."""
import random

import pytest

from common import rules
from common.config import load_sectors
from sim_model import SectorSim

SECTORS = load_sectors()


def window(sim, hour, seconds=10):
    """Simule `seconds` mesures à 1 Hz et retourne l'agrégat de la fenêtre (comme Spark)."""
    msgs = [sim.step(hour, 1.0, 0) for _ in range(seconds)]
    n = len(msgs)
    return {
        "avg_pressure": sum(m["pressure_bar"] for m in msgs) / n,
        "avg_flow": sum(m["flow_m3h"] for m in msgs) / n,
        "avg_turbidity": sum(m["turbidity_ntu"] for m in msgs) / n,
        "sim_hour": msgs[-1]["sim_hour"],
    }


@pytest.mark.parametrize("sector", SECTORS, ids=lambda s: s["id"])
def test_no_false_alarm_all_day(sector):
    sim = SectorSim(sector, random.Random(7))
    for hour in range(24):
        for _ in range(3):
            assert rules.classify(window(sim, hour + 0.5), sector)["state"] == "OK", f"fausse alerte à {hour}h"


@pytest.mark.parametrize("fault,state", [("leak", "LEAK"), ("outage", "OUTAGE"), ("contamination", "QUALITY")])
@pytest.mark.parametrize("hour", [3.0, 8.0, 14.0, 19.0])
def test_fault_is_detected(fault, state, hour):
    sector = SECTORS[0]
    sim = SectorSim(sector, random.Random(3))
    window(sim, hour, 5)
    sim.command(fault)
    window(sim, hour, 16)                       # la panne atteint son intensité maximale
    res = rules.classify(window(sim, hour, 10), sector)
    assert res["state"] == state
    assert res["severity"] >= 1


def test_leak_escalates_and_reports_loss():
    sector = SECTORS[5]
    sim = SectorSim(sector, random.Random(11))
    window(sim, 10.0, 5)
    sim.command("leak")
    severities = [rules.classify(window(sim, 10.0, 4), sector)["severity"] for _ in range(6)]
    assert severities[-1] == 3
    assert max(severities) >= severities[0]
    assert rules.classify(window(sim, 10.0, 4), sector)["est_loss_m3h"] > 0


def test_repair_returns_to_ok():
    sector = SECTORS[2]
    sim = SectorSim(sector, random.Random(5))
    sim.command("outage")
    window(sim, 9.0, 10)
    assert rules.classify(window(sim, 9.0, 8), sector)["state"] == "OUTAGE"
    sim.command("repair")
    window(sim, 9.0, 8)
    assert rules.classify(window(sim, 9.0, 10), sector)["state"] == "OK"


def test_reservoir_drains_during_outage_and_recovers():
    sim = SectorSim(SECTORS[1], random.Random(2))
    start = sim.level
    sim.command("outage")
    for _ in range(60):
        sim.step(9.0, 1.0, 0)
    assert sim.level < start - 15
    sim.command("repair")
    for _ in range(90):
        sim.step(9.0, 1.0, 0)
    assert sim.level > start - 10
