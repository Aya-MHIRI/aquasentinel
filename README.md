# AquaSentinel 💧

Détection **en quasi temps réel** des fuites, coupures et anomalies de qualité sur un réseau de distribution d'eau, avec analyse historique des secteurs les plus fragiles.
Projet Big Data (II3) · architecture **Lambda simplifiée** : une couche rapide (streaming) et une couche batch.

> ⚠️ Les capteurs sont **simulés** (secteurs hydrauliques virtuels nommés d'après des quartiers du Grand Tunis). Le simulateur injecte des pannes avec une vérité terrain, ce qui permet de valider la détection.

## Le problème

En Tunisie, une part importante de l'eau produite est perdue dans les fuites, et les coupures sont souvent connues quand les habitants se plaignent. AquaSentinel détecte l'anomalie en quelques secondes, dit **où** et **de quel type**, estime l'eau perdue, et classe les secteurs fragiles à partir de l'historique.

## Architecture

```
                         ┌───────────────────────────── couche rapide ─────────────────────────────┐
Capteurs simulés ──► Kafka (water.sensors) ──► Spark Structured Streaming ──► MongoDB ──► Dashboard
 (Python, 1 Hz)            │                    fenêtres 20 s + règles        état, alertes   FastAPI + Leaflet
       ▲                   │                                                  métriques        (SSE)
       │                   └──► HDFS (Parquet) ──► Spark batch + MLlib ──────► MongoDB (classement)
       │                              couche batch : score de fragilité + KMeans
       └──── Kafka (water.commands) ◄──── bouton « injecter une panne » du dashboard
```

| Brique | Rôle | Pourquoi |
|---|---|---|
| **Simulateur Python** | Pression, débit, niveau de réservoir, turbidité par secteur ; pannes à rampe progressive | Source contrôlée et reproductible, avec vérité terrain |
| **Kafka** (KRaft) | Collecte, tampon, découplage | Absorbe les pics, conserve et rejoue les messages ; sert aussi de canal de commandes |
| **Spark Structured Streaming** | Fenêtres glissantes 20 s / pas 10 s par secteur, règles de détection | Même API que le batch, tolérance aux pannes (checkpoints), watermark pour les données tardives |
| **HDFS (Parquet)** | Archive brute | Volume croissant, réplication, format colonnaire efficace pour l'analytique |
| **Spark batch + MLlib** | Score de fragilité 0-100, profils par KMeans | Traite tout l'historique ; le débit nocturne minimal est l'indicateur classique des fuites |
| **MongoDB** | Base NoSQL de service | Documents flexibles, écritures rapides, TTL pour l'historique court, upserts par secteur |
| **FastAPI + Leaflet + SSE** | Salle de contrôle en direct | Poussée temps réel sans polling, injection de pannes en démo |
| **Docker Compose** | Environnement | Un seul `docker compose up` |

### Règles de détection (`common/rules.py`)
- **Coupure** : débit < 5 % du nominal **et** pression < 0,5 bar.
- **Fuite** : pression < 88 % du nominal **et** débit > 110 % du débit attendu à cette heure (profil de demande horaire).
- **Qualité** : turbidité > 4 NTU.
- Sévérité 1-3 selon l'ampleur de la chute de pression ; perte d'eau estimée = débit mesuré − débit attendu.

## Lancer le projet

Prérequis : Docker avec ~6 Go de RAM disponibles.

```bash
docker compose up --build
```

Le premier démarrage télécharge les images et les paquets Spark (quelques minutes). Ensuite :

| Interface | URL |
|---|---|
| **Dashboard AquaSentinel** | http://localhost:8000 |
| HDFS NameNode | http://localhost:9870 |

La carte utilise Leaflet et les tuiles CARTO via Internet : sans connexion, les secteurs, alertes et jauges fonctionnent mais le fond de carte reste sombre et vide.

### Scénario de démo (2 minutes)
1. Ouvrir le dashboard : les 10 secteurs sont verts, l'heure simulée défile (un jour = 4 min).
2. Choisir un secteur, cliquer **Fuite** : la pression chute, le secteur passe à l'orange puis au rouge, une alerte apparaît, le réservoir se vide. Un message affiche le **temps de détection** de bout en bout.
3. **Coupure** et **Contamination** : autres types d'anomalie. **Réparer** ramène le secteur à la normale.
4. Après quelques minutes, le panneau « Secteurs les plus fragiles » se remplit (job batch sur HDFS + KMeans).

Pour des pannes automatiques en démo continue, mettre `AUTO_FAULTS_EVERY: "40"` dans le service `simulator`.

### Si HDFS pose problème
Remplacer `RAW_PATH` et `CHECKPOINT_RAW` par `file:///data/raw` et `file:///data/chk/raw` dans les services Spark et monter un volume `./data:/data`. Le code est identique, seul le stockage change.

## Tests (sans Docker)

```bash
pip install pytest mongomock fastapi httpx confluent-kafka pyspark==3.5.1 numpy
python -m pytest tests            # règles, simulateur, API du dashboard
python tests/spark_local_check.py # vrais jobs Spark en local (fichiers JSON à la place de Kafka)
```

- `tests/test_detection.py` : aucune fausse alerte sur 24 h pour les 10 secteurs, détection de chaque type de panne à plusieurs heures, retour à la normale après réparation.
- `tests/test_api.py` : API du dashboard sur une base simulée.
- `tests/spark_local_check.py` : `parse_events`, `aggregate`, `process_batch` et `batch_job` exécutés pour de vrai avec Spark local.

## Organisation du dépôt

```
common/      règles de détection, profil de demande, configuration des secteurs (partagé)
simulator/   modèle physique des secteurs + producteur Kafka + écoute des commandes
spark/       streaming_job.py (couche rapide), batch_job.py (couche batch + MLlib)
dashboard/   API FastAPI + interface Leaflet
tests/       tests unitaires et vérification Spark locale
docs/        modèle de déclaration d'usage de l'IA
```

## Répartition du travail (à adapter à votre trinôme)

| Membre | Responsabilité | Fichiers principaux |
|---|---|---|
| 1 | Ingestion : simulateur, Kafka, docker-compose | `simulator/`, `docker-compose.yml` |
| 2 | Streaming : fenêtres, règles, alertes, sink MongoDB | `spark/streaming_job.py`, `common/rules.py` |
| 3 | Batch et restitution : HDFS, MLlib, dashboard | `spark/batch_job.py`, `dashboard/` |

L'oral est individuel et porte sur le code : chacun doit savoir expliquer **tout** le pipeline, pas seulement sa partie.

## À préparer pour le rendu
- [ ] Diapos (8 max) : problème, architecture, justification de chaque technologie, lien GitHub, répartition du travail
- [ ] Courte vidéo de démo
- [ ] Déclaration d'usage de l'IA signée (modèle dans `docs/declaration_usage_ia.md`)
