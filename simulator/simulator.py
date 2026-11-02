"""Simulateur de capteurs : publie les mesures sur Kafka et écoute les commandes d'injection de pannes.

Topics :
  water.sensors   (sortie)  une mesure par secteur et par seconde
  water.commands  (entrée)  {"sector_id": "...", "fault": "leak|outage|contamination|repair"}
"""
import json
import os
import random
import threading
import time
import uuid

from confluent_kafka import Consumer, Producer

from common.config import load_sectors
from sim_model import VALID_FAULTS, SectorSim

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_SENSORS = os.environ.get("TOPIC_SENSORS", "water.sensors")
TOPIC_COMMANDS = os.environ.get("TOPIC_COMMANDS", "water.commands")
SIM_SPEED = float(os.environ.get("SIM_SPEED", "360"))        # secondes simulées par seconde réelle
START_HOUR = float(os.environ.get("SIM_START_HOUR", "6"))
TICK = float(os.environ.get("TICK_SECONDS", "1"))
AUTO_FAULTS_EVERY = float(os.environ.get("AUTO_FAULTS_EVERY", "0"))

lock = threading.Lock()


def listen_commands(sims: dict) -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"simulator-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_COMMANDS])
    print(f"[sim] écoute des commandes sur {TOPIC_COMMANDS}", flush=True)
    while True:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        try:
            cmd = json.loads(msg.value())
            fault, target = cmd["fault"], cmd["sector_id"]
            if fault not in VALID_FAULTS | {"repair"}:
                continue
            with lock:
                targets = list(sims) if target == "*" else [target]
                for sid in targets:
                    if sid in sims:
                        sims[sid].command(fault)
            print(f"[sim] commande reçue : {fault} -> {target}", flush=True)
        except Exception as exc:  # message mal formé : on l'ignore
            print(f"[sim] commande ignorée ({exc})", flush=True)


def main() -> None:
    sectors = load_sectors()
    sims = {s["id"]: SectorSim(s) for s in sectors}
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})

    threading.Thread(target=listen_commands, args=(sims,), daemon=True).start()

    t0 = last = time.time()
    next_auto = t0 + AUTO_FAULTS_EVERY if AUTO_FAULTS_EVERY > 0 else None
    pending_repairs = []
    ticks = 0
    print(f"[sim] démarrage : {len(sims)} secteurs, vitesse x{SIM_SPEED:g}, broker {BOOTSTRAP}", flush=True)

    while True:
        now = time.time()
        dt, last = now - last, now
        sim_hour = (START_HOUR + (now - t0) * SIM_SPEED / 3600.0) % 24

        with lock:
            if next_auto and now >= next_auto:
                sid = random.choice(list(sims))
                fault = random.choice(sorted(VALID_FAULTS))
                sims[sid].command(fault)
                pending_repairs.append((now + 45, sid))
                next_auto = now + AUTO_FAULTS_EVERY
                print(f"[sim] panne automatique : {fault} sur {sid}", flush=True)
            for item in [p for p in pending_repairs if p[0] <= now]:
                sims[item[1]].command("repair")
                pending_repairs.remove(item)

            batch = [s.step(sim_hour, dt, int(now * 1000)) for s in sims.values()]

        for m in batch:
            producer.produce(TOPIC_SENSORS, key=m["sector_id"], value=json.dumps(m))
        producer.poll(0)

        ticks += 1
        if ticks % 30 == 0:
            print(f"[sim] {ticks} ticks envoyés, heure simulée {int(sim_hour):02d}:{int(sim_hour % 1 * 60):02d}", flush=True)

        time.sleep(max(0.0, TICK - (time.time() - now)))


if __name__ == "__main__":
    main()
