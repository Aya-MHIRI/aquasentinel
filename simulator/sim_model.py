"""Modèle physique simplifié d'un secteur du réseau d'eau (sans dépendance Kafka, donc testable).

Chaque secteur produit à chaque tick : pression, débit, niveau du réservoir, turbidité.
Une panne (fuite / coupure / contamination) monte progressivement en intensité (rampe),
ce qui permet de voir l'alerte passer d'orange à rouge pendant la démo.
"""
import math
import random

from common.demand import demand_factor

# Durée (en secondes réelles) pour atteindre l'intensité maximale
RAMP_SECONDS = {"leak": 15.0, "outage": 6.0, "contamination": 12.0}
REPAIR_SECONDS = 5.0
VALID_FAULTS = set(RAMP_SECONDS)


class SectorSim:
    def __init__(self, cfg: dict, rng: random.Random = None):
        self.cfg = cfg
        self.rng = rng or random.Random()
        self.fault = "none"
        self.intensity = 0.0
        self.target = 0          # 1 = la panne s'installe, 0 = réparation en cours
        self.level = 72.0 + self.rng.uniform(-6, 6)

    # -------------------------------------------------------------- commandes
    def command(self, fault: str) -> None:
        if fault == "repair":
            if self.fault != "none":
                self.target = 0
            return
        if fault not in VALID_FAULTS:
            raise ValueError(f"panne inconnue : {fault}")
        if fault != self.fault:
            self.intensity = 0.0
        self.fault = fault
        self.target = 1

    def _advance(self, dt: float) -> None:
        if self.fault == "none":
            return
        if self.target == 1:
            self.intensity = min(1.0, self.intensity + dt / RAMP_SECONDS[self.fault])
        else:
            self.intensity = max(0.0, self.intensity - dt / REPAIR_SECONDS)
            if self.intensity <= 0.0:
                self.fault = "none"

    # -------------------------------------------------------------- mesure
    def step(self, sim_hour: float, dt: float, now_ms: int) -> dict:
        self._advance(dt)
        c, rng, i = self.cfg, self.rng, self.intensity
        nom_p, nom_f = c["nominal_pressure_bar"], c["nominal_flow_m3h"]

        d = demand_factor(sim_hour)
        pressure = nom_p * (1.04 - 0.08 * d)       # la pression baisse un peu quand la demande monte
        flow = nom_f * d
        turbidity = 0.6 + abs(rng.gauss(0, 0.08))
        drift = 0.0                                 # variation du niveau du réservoir (%/s)

        if self.fault == "leak":
            pressure *= 1 - 0.40 * i
            flow += nom_f * 0.45 * i
            drift = -0.30 * i
        elif self.fault == "outage":
            pressure *= 1 - 0.97 * i
            flow *= 1 - 0.99 * i
            drift = -0.50 * i
        elif self.fault == "contamination":
            turbidity += 8.5 * i

        pressure = max(0.0, pressure + rng.gauss(0, 0.02 * nom_p))
        flow = max(0.0, flow * (1 + rng.gauss(0, 0.02)))

        # Le réservoir revient vers son niveau cible, sauf pendant une panne
        level_target = 72 + 8 * math.sin(2 * math.pi * (sim_hour - 6) / 24)
        pull = min(1.0, 0.05 * dt) * (1 - i)
        self.level += (level_target - self.level) * pull + drift * dt
        self.level = min(100.0, max(3.0, self.level))

        return {
            "sector_id": c["id"],
            "ts_ms": now_ms,
            "sim_hour": round(sim_hour, 4),
            "pressure_bar": round(pressure, 3),
            "flow_m3h": round(flow, 1),
            "reservoir_pct": round(self.level, 1),
            "turbidity_ntu": round(turbidity, 2),
            "fault": self.fault if i > 0 else "none",   # vérité terrain (pour valider la détection)
        }
