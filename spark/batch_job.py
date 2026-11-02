"""Couche batch : lit l'historique Parquet sur HDFS, calcule un score de fragilité par secteur
et regroupe les secteurs en profils avec KMeans (MLlib). Le résultat est écrit dans MongoDB
(collection sector_ranking) et affiché par le dashboard.

Indicateurs utilisés :
  - pressure_ratio    : pression moyenne / pression nominale
  - night_flow_ratio  : débit entre 2h et 4h / débit nocturne attendu (le débit minimal de nuit
                        est l'indicateur classique des fuites dans un réseau d'eau)
  - low_pressure_rate : part du temps en pression basse
  - outage_rate       : part du temps en coupure
  - turbidity_rate    : part du temps avec turbidité anormale
"""
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import rules
from common.config import load_sectors
from common.demand import demand_factor

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
RAW_PATH = os.environ.get("RAW_PATH", "hdfs://namenode:9000/aqua/raw")

NIGHT_DEMAND = sum(demand_factor(2 + 0.25 * k) for k in range(8)) / 8   # demande moyenne 2h-4h


def norm(col, cap):
    """Ramène une colonne dans [0, 1] (1 = au-delà du plafond `cap`)."""
    return F.least(F.lit(1.0), F.greatest(F.lit(0.0), col / F.lit(cap)))


def main():
    spark = SparkSession.builder.appName("AquaSentinel-Batch").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.option("basePath", RAW_PATH).parquet(RAW_PATH)
        total = df.count()
    except Exception as exc:
        print(f"[batch] pas encore de données exploitables sur {RAW_PATH} ({type(exc).__name__}), on réessaiera.")
        sys.exit(0)
    if total == 0:
        print("[batch] historique vide, on réessaiera.")
        sys.exit(0)

    sectors = load_sectors()
    ref = spark.createDataFrame(
        [(s["id"], s["name"], float(s["nominal_pressure_bar"]), float(s["nominal_flow_m3h"])) for s in sectors],
        ["sector_id", "name", "nom_p", "nom_f"],
    )
    j = df.join(F.broadcast(ref), "sector_id")

    night = (F.col("sim_hour") >= 2) & (F.col("sim_hour") < 4)
    feat = (j.groupBy("sector_id", "name").agg(
        F.count("*").alias("n_readings"),
        F.avg("pressure_bar").alias("avg_pressure"),
        F.avg(F.when(night, F.col("flow_m3h") / (F.col("nom_f") * NIGHT_DEMAND))).alias("night_flow_ratio"),
        F.avg((F.col("pressure_bar") < rules.LEAK_PRESSURE_RATIO * F.col("nom_p")).cast("double")).alias("low_pressure_rate"),
        F.avg((F.col("flow_m3h") < rules.OUTAGE_FLOW_RATIO * F.col("nom_f")).cast("double")).alias("outage_rate"),
        F.avg((F.col("turbidity_ntu") > rules.TURBIDITY_LIMIT_NTU).cast("double")).alias("turbidity_rate"),
        F.first("nom_p").alias("nom_p"),
    ).na.fill({"night_flow_ratio": 1.0}))

    feat = feat.withColumn("pressure_ratio", F.col("avg_pressure") / F.col("nom_p"))
    feat = feat.withColumn("score", F.round(100 * (
        0.35 * norm(F.col("low_pressure_rate"), 0.20)
        + 0.30 * norm(F.col("night_flow_ratio") - 1.0, 1.0)
        + 0.20 * norm(F.col("outage_rate"), 0.10)
        + 0.15 * norm(F.col("turbidity_rate"), 0.10)
    ), 1))

    # Profils de secteurs par KMeans (MLlib)
    cols = ["pressure_ratio", "night_flow_ratio", "low_pressure_rate", "outage_rate", "turbidity_rate"]
    vec = VectorAssembler(inputCols=cols, outputCol="raw").transform(feat)
    scaled = StandardScaler(inputCol="raw", outputCol="features", withStd=True, withMean=False).fit(vec).transform(vec)
    k = max(1, min(3, scaled.count()))
    pred = KMeans(k=k, seed=42, featuresCol="features").fit(scaled).transform(scaled)

    rows = [r.asDict() for r in pred.select("sector_id", "name", "score", "prediction", "avg_pressure",
                                            "night_flow_ratio", "low_pressure_rate", "outage_rate",
                                            "turbidity_rate", "n_readings").collect()]

    # Ordonne les clusters du moins au plus fragile (score moyen) et leur donne un nom
    by_cluster = {}
    for r in rows:
        by_cluster.setdefault(r["prediction"], []).append(r["score"])
    ordered = sorted(by_cluster, key=lambda c: sum(by_cluster[c]) / len(by_cluster[c]))
    names = {1: ["Stable"], 2: ["Stable", "Fragile"], 3: ["Stable", "À surveiller", "Fragile"]}[len(ordered)]
    label = {c: names[i] for i, c in enumerate(ordered)}

    now = datetime.now(timezone.utc)
    docs = [{
        "sector_id": r["sector_id"], "name": r["name"], "score": r["score"],
        "profile": label[r["prediction"]], "cluster": int(r["prediction"]),
        "avg_pressure": round(r["avg_pressure"], 2), "night_flow_ratio": round(r["night_flow_ratio"], 2),
        "low_pressure_rate": round(r["low_pressure_rate"], 4), "n_readings": r["n_readings"],
        "computed_at": now,
    } for r in rows]

    ranking = MongoClient(MONGO_URI)["aquasentinel"]["sector_ranking"]
    ranking.delete_many({})
    ranking.insert_many(docs)
    print(f"[batch] classement mis à jour : {len(docs)} secteurs, {total} mesures analysées")
    spark.stop()


if __name__ == "__main__":
    main()
