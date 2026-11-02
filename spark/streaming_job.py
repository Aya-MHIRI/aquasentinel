"""Couche rapide (speed layer) : Kafka -> Spark Structured Streaming -> MongoDB + archivage HDFS.

Deux requêtes tournent en parallèle sur le même flux Kafka :
  1. archive  : écrit chaque mesure brute en Parquet sur HDFS (alimente la couche batch)
  2. détection: agrégats par fenêtre glissante (20 s, pas de 10 s) et secteur, puis application
                des règles (common/rules.py) et écriture de l'état + des alertes dans MongoDB
"""
import os
from datetime import datetime, timezone

from pymongo import ASCENDING, DESCENDING, MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import rules
from common.config import sectors_by_id

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
RAW_PATH = os.environ.get("RAW_PATH", "hdfs://namenode:9000/aqua/raw")
CHECKPOINT_RAW = os.environ.get("CHECKPOINT_RAW", "hdfs://namenode:9000/aqua/chk/raw")
CHECKPOINT_AGG = os.environ.get("CHECKPOINT_AGG", "/tmp/chk/agg")
MIN_SAMPLES = 4   # une fenêtre doit contenir au moins 4 mesures pour être jugée

SCHEMA = T.StructType([
    T.StructField("sector_id", T.StringType()),
    T.StructField("ts_ms", T.LongType()),
    T.StructField("sim_hour", T.DoubleType()),
    T.StructField("pressure_bar", T.DoubleType()),
    T.StructField("flow_m3h", T.DoubleType()),
    T.StructField("reservoir_pct", T.DoubleType()),
    T.StructField("turbidity_ntu", T.DoubleType()),
    T.StructField("fault", T.StringType()),
])

SECTORS = sectors_by_id()
_db = None


def get_db():
    global _db
    if _db is None:
        _db = MongoClient(MONGO_URI)["aquasentinel"]
        _db.metrics.create_index("ts", expireAfterSeconds=3600)   # historique court (1 h)
        _db.metrics.create_index([("sector_id", ASCENDING), ("ts", DESCENDING)])
        _db.alerts.create_index([("ts", DESCENDING)])
    return _db


def emit_alert(db, sector, prev, res, now):
    """Crée une alerte quand l'état change ou que la sévérité augmente."""
    prev_state = prev["state"] if prev else "OK"
    prev_sev = prev.get("severity", 0) if prev else 0
    if res["state"] == prev_state and not (res["state"] != "OK" and res["severity"] > prev_sev):
        return
    if res["state"] == "OK":
        kind, message = "resolved", "Retour à la normale"
    else:
        kind, message = ("open" if prev_state == "OK" else "escalation"), res["message"]
    db.alerts.insert_one({
        "sector_id": sector["id"],
        "name": sector["name"],
        "type": res["state"] if res["state"] != "OK" else prev_state,
        "severity": res["severity"],
        "kind": kind,
        "message": message,
        "ts": now,
    })


def process_batch(batch_df, batch_id):
    rows = [r.asDict(recursive=True) for r in batch_df.collect()]
    if not rows:
        return

    # Pour chaque secteur : la fenêtre la plus récente qui contient assez de mesures
    latest = {}
    for r in rows:
        key = (r["n"] >= MIN_SAMPLES, r["window"]["end"])
        cur = latest.get(r["sector_id"])
        if cur is None or key > cur[0]:
            latest[r["sector_id"]] = (key, r)

    now = datetime.now(timezone.utc)
    db = get_db()
    for sid, (_, r) in latest.items():
        sector = SECTORS.get(sid)
        if sector is None:
            continue
        res = rules.classify(r, sector)
        prev = db.sector_state.find_one({"_id": sid})

        db.sector_state.replace_one({"_id": sid}, {
            "_id": sid,
            "state": res["state"],
            "severity": res["severity"],
            "message": res["message"],
            "est_loss_m3h": res["est_loss_m3h"],
            "expected_flow": res["expected_flow"],
            "pressure": round(r["avg_pressure"], 3),
            "min_pressure": round(r["min_pressure"], 3),
            "flow": round(r["avg_flow"], 1),
            "level": round(r["avg_level"], 1),
            "turbidity": round(r["avg_turbidity"], 2),
            "sim_hour": round(r["sim_hour"], 3),
            "samples": r["n"],
            "updated_at": now,
        }, upsert=True)

        db.metrics.insert_one({
            "sector_id": sid, "ts": now, "state": res["state"],
            "pressure": round(r["avg_pressure"], 3), "flow": round(r["avg_flow"], 1),
            "level": round(r["avg_level"], 1), "turbidity": round(r["avg_turbidity"], 2),
        })
        emit_alert(db, sector, prev, res, now)


def parse_events(df_json):
    """Colonne `value` (JSON texte) -> colonnes typées + temps d'événement."""
    return (df_json.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("m"))
            .select("m.*")
            .where(F.col("sector_id").isNotNull())
            .withColumn("event_time", (F.col("ts_ms") / 1000).cast("timestamp"))
            .withColumn("date", F.to_date("event_time")))


def aggregate(parsed):
    """Fenêtres glissantes de 20 s (pas de 10 s) par secteur."""
    return (parsed.withWatermark("event_time", "30 seconds")
            .groupBy(F.window("event_time", "20 seconds", "10 seconds"), "sector_id")
            .agg(F.avg("pressure_bar").alias("avg_pressure"),
                 F.min("pressure_bar").alias("min_pressure"),
                 F.avg("flow_m3h").alias("avg_flow"),
                 F.avg("reservoir_pct").alias("avg_level"),
                 F.avg("turbidity_ntu").alias("avg_turbidity"),
                 F.max("sim_hour").alias("sim_hour"),
                 F.count("*").alias("n")))


def main():
    spark = SparkSession.builder.appName("AquaSentinel-Streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    get_db()

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "water.sensors")
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    parsed = parse_events(raw)

    # 1) Archivage brut sur HDFS (Parquet partitionné par date)
    (parsed.writeStream.format("parquet")
     .option("path", RAW_PATH)
     .option("checkpointLocation", CHECKPOINT_RAW)
     .partitionBy("date")
     .trigger(processingTime="30 seconds")
     .start())

    # 2) Détection : fenêtres glissantes par secteur
    agg = aggregate(parsed)

    (agg.writeStream.outputMode("update")
     .foreachBatch(process_batch)
     .option("checkpointLocation", CHECKPOINT_AGG)
     .trigger(processingTime="2 seconds")
     .start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
