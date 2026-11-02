"""Vérification de bout en bout des jobs Spark SANS Kafka, HDFS ni MongoDB réels.

  - streaming : des fichiers JSON simulés remplacent Kafka ; MongoDB est remplacé par mongomock.
                Le code testé est le vrai (parse_events, aggregate, process_batch, rules).
  - batch     : un jour de mesures simulées est écrit en Parquet local puis lu par batch_job.

Usage :  python tests/spark_local_check.py      (nécessite pyspark, mongomock, numpy, Java)
"""
import json
import os
import random
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [ROOT, os.path.join(ROOT, "simulator"), os.path.join(ROOT, "spark")]

import mongomock  # noqa: E402
import pymongo  # noqa: E402

shared = mongomock.MongoClient(tz_aware=True)
pymongo.MongoClient = lambda *a, **k: shared          # tous les jobs parlent à la même fausse base

tmp = tempfile.mkdtemp(prefix="aqua_check_")
os.environ["RAW_PATH"] = os.path.join(tmp, "raw")

from pyspark.sql import SparkSession  # noqa: E402

from common.config import load_sectors  # noqa: E402
from sim_model import SectorSim  # noqa: E402

import batch_job as bj  # noqa: E402
import streaming_job as sj  # noqa: E402

spark = (SparkSession.builder.master("local[2]").appName("aqua-check")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
db = shared["aquasentinel"]
SECTORS = load_sectors()
failures = []


def check(cond, label):
    print(("  OK   " if cond else "  FAIL ") + label)
    if not cond:
        failures.append(label)


# ------------------------------------------------------------------ streaming
print("== Streaming : fuite injectée sur Ariana à t = 30 s")
sims = {s["id"]: SectorSim(s, random.Random(i)) for i, s in enumerate(SECTORS)}
indir = os.path.join(tmp, "in")
os.makedirs(indir)
t0 = int(time.time() * 1000) - 300_000
for chunk in range(6):                                   # 6 fichiers de 15 s = 90 s de données
    lines = []
    for k in range(15):
        sec = chunk * 15 + k
        if sec == 30:
            sims["ariana"].command("leak")
        for s in sims.values():
            lines.append(json.dumps(s.step(10.0, 1.0, t0 + sec * 1000)))
    with open(os.path.join(indir, f"{chunk:03d}.json"), "w") as f:
        f.write("\n".join(lines))

raw = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(indir)
agg = sj.aggregate(sj.parse_events(raw))
q = (agg.writeStream.outputMode("update").foreachBatch(sj.process_batch)
     .option("checkpointLocation", os.path.join(tmp, "chk")).trigger(availableNow=True).start())
q.awaitTermination(180)

states = {d["_id"]: d for d in db.sector_state.find()}
alerts = list(db.alerts.find())
check(len(states) == 10, f"10 secteurs ont un état ({len(states)})")
check(states["ariana"]["state"] == "LEAK" and states["ariana"]["severity"] >= 2,
      f"Ariana en fuite (état={states['ariana']['state']}, sévérité={states['ariana']['severity']})")
check(all(s["state"] == "OK" for k, s in states.items() if k != "ariana"), "les 9 autres secteurs restent nominaux")
check(any(a["sector_id"] == "ariana" and a["kind"] == "open" and a["type"] == "LEAK" for a in alerts),
      "une alerte 'fuite' a été ouverte pour Ariana")
check(all(a["sector_id"] == "ariana" for a in alerts), f"aucune fausse alerte ailleurs ({len(alerts)} alertes au total)")
check(states["ariana"]["est_loss_m3h"] > 100, f"perte estimée Ariana : {states['ariana']['est_loss_m3h']} m³/h")
check(db.metrics.count_documents({}) > 0, "historique des métriques alimenté")

# ------------------------------------------------------------------ batch
print("== Batch : un jour simulé, fuite nocturne à Ariana, coupure au Bardo, contamination au Lac")
rows = []
for i, s in enumerate(SECTORS):
    sim = SectorSim(s, random.Random(100 + i))
    for tick in range(1440):                              # 1 tick = 1 minute simulée
        if s["id"] == "ariana":
            if tick == 90:
                sim.command("leak")
            if tick == 270:
                sim.command("repair")
        if s["id"] == "bardo":
            if tick == 600:
                sim.command("outage")
            if tick == 640:
                sim.command("repair")
        if s["id"] == "lac1":
            if tick == 900:
                sim.command("contamination")
            if tick == 930:
                sim.command("repair")
        m = sim.step(tick / 60.0, 1.0, tick * 1000)
        m["date"] = "2026-10-28"
        rows.append(m)
spark.createDataFrame(rows).write.partitionBy("date").parquet(os.environ["RAW_PATH"])

bj.main()                                                 # lit le Parquet, KMeans, écrit dans MongoDB (spark.stop() à la fin)
ranking = sorted(db.sector_ranking.find(), key=lambda r: -r["score"])
for r in ranking:
    print(f"     {r['name']:<18} score={r['score']:>5}  profil={r['profile']:<13} night_ratio={r['night_flow_ratio']}")
check(len(ranking) == 10, "10 secteurs classés")
check(ranking[0]["sector_id"] == "ariana", "Ariana (fuite nocturne) est le secteur le plus fragile")
check({r["sector_id"] for r in ranking[:3]} == {"ariana", "bardo", "lac1"}, "top 3 = les 3 secteurs touchés")
check(ranking[0]["night_flow_ratio"] > 1.5, "ratio de débit nocturne élevé pour Ariana")
check(ranking[0]["profile"] != "Stable" and ranking[-1]["profile"] == "Stable", "profils KMeans cohérents")

print("\nRÉSULTAT :", "TOUT EST OK" if not failures else f"{len(failures)} ÉCHEC(S)")
sys.exit(1 if failures else 0)
