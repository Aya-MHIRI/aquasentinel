"""Chargement de la configuration des secteurs du réseau (partagée par tous les services)."""
import json
import os

_DIR = os.path.dirname(os.path.abspath(__file__))


def load_sectors(path=None):
    """Retourne la liste des secteurs définis dans sectors.json."""
    path = path or os.environ.get("SECTORS_FILE", os.path.join(_DIR, "sectors.json"))
    with open(path, encoding="utf-8") as f:
        return json.load(f)["sectors"]


def sectors_by_id(path=None):
    return {s["id"]: s for s in load_sectors(path)}
