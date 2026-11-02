"""Profil de demande horaire (facteur multiplicatif du débit nominal d'un secteur).

Pointes le matin (7h-8h) et le soir (18h-19h), creux la nuit (2h-4h).
Le simulateur génère le débit avec ce profil ; les règles de détection l'utilisent
pour savoir quel débit est "attendu" à une heure donnée.
"""

HOURLY = [
    0.35, 0.30, 0.28, 0.28, 0.32, 0.50,   # 00h - 05h
    0.90, 1.25, 1.15, 1.00, 0.95, 1.00,   # 06h - 11h
    1.10, 1.05, 0.95, 0.90, 0.95, 1.10,   # 12h - 17h
    1.25, 1.20, 1.00, 0.80, 0.55, 0.42,   # 18h - 23h
]


def demand_factor(hour: float) -> float:
    """Interpolation linéaire du profil horaire. `hour` en heures décimales (0-24)."""
    h = hour % 24
    i = int(h)
    frac = h - i
    return HOURLY[i] * (1 - frac) + HOURLY[(i + 1) % 24] * frac
