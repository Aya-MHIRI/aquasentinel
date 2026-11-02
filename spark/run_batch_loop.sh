#!/bin/bash
# Relance le job batch à intervalle régulier (le temps que l'historique HDFS grossisse).
until (echo > /dev/tcp/namenode/9000) 2>/dev/null; do echo "attente HDFS..."; sleep 3; done
sleep 30
while true; do
  /opt/spark/bin/spark-submit --master "local[2]" \
    --conf spark.jars.ivy=/tmp/.ivy \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.sql.shuffle.partitions=4 \
    /app/spark/batch_job.py || echo "[batch] échec, nouvel essai au prochain cycle"
  sleep "${BATCH_INTERVAL:-90}"
done
