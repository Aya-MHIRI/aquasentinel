"""Règles de détection d'anomalies (fonction pure, testable sans Spark).

Entrée : une agrégation par fenêtre glissante d'un secteur
    {avg_pressure, avg_flow, avg_turbidity, sim_hour}
Sortie : état du secteur, sévérité (0-3), message en français, perte d'eau estimée.

États : OK | LEAK (fuite) | OUTAGE (coupure) | QUALITY (turbidité anormale)
"""
from .demand import demand_factor

# Seuils (modifiables, à justifier à l'oral)
LEAK_PRESSURE_RATIO = 0.88      # pression < 88 % de la pression nominale ...
LEAK_FLOW_RATIO = 1.10          # ... ET débit > 110 % du débit attendu à cette heure
OUTAGE_FLOW_RATIO = 0.05        # débit < 5 % du nominal ...
OUTAGE_PRESSURE_BAR = 0.5       # ... ET pression < 0,5 bar
TURBIDITY_LIMIT_NTU = 4.0       # turbidité au-dessus de laquelle l'eau est suspecte


def classify(agg: dict, sector: dict) -> dict:
    nominal_p = sector["nominal_pressure_bar"]
    nominal_f = sector["nominal_flow_m3h"]
    expected_f = nominal_f * demand_factor(agg["sim_hour"])

    p = agg["avg_pressure"]
    f = agg["avg_flow"]
    t = agg["avg_turbidity"]

    result = {
        "state": "OK",
        "severity": 0,
        "message": "Fonctionnement nominal",
        "est_loss_m3h": 0.0,
        "expected_flow": round(expected_f, 1),
    }

    if f < OUTAGE_FLOW_RATIO * nominal_f and p < OUTAGE_PRESSURE_BAR:
        result.update(
            state="OUTAGE",
            severity=3,
            message=f"Coupure d'eau : débit quasi nul ({f:.0f} m³/h), pression {p:.2f} bar",
        )
    elif p < nominal_p * LEAK_PRESSURE_RATIO and f > expected_f * LEAK_FLOW_RATIO:
        drop = 1 - p / nominal_p
        severity = 1 if drop < 0.20 else 2 if drop < 0.30 else 3
        loss = max(0.0, f - expected_f)
        result.update(
            state="LEAK",
            severity=severity,
            est_loss_m3h=round(loss, 1),
            message=(
                f"Fuite probable : pression {p:.2f} bar (-{drop:.0%}), "
                f"débit {f:.0f} m³/h pour {expected_f:.0f} attendus"
            ),
        )
    elif t > TURBIDITY_LIMIT_NTU:
        result.update(
            state="QUALITY",
            severity=2 if t < 2 * TURBIDITY_LIMIT_NTU else 3,
            message=f"Turbidité anormale : {t:.1f} NTU (seuil {TURBIDITY_LIMIT_NTU:.0f} NTU)",
        )
    return result
