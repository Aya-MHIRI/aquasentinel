# AquaSentinel 💧

Détection **en quasi temps réel** des fuites, coupures et anomalies de qualité de l'eau sur un réseau de distribution, avec analyse historique des secteurs les plus fragiles.

Projet **Big Data (II3)** · architecture **Lambda simplifiée** : une couche rapide (streaming) et une couche batch.

**Équipe** : Aya Mhiri · Sarra Bouraoui · Ella Ibrahim

> ⚠️ Les capteurs sont **simulés** : dix secteurs hydrauliques virtuels, nommés d'après des quartiers du Grand Tunis. Un simulateur Python produit les mesures et injecte des pannes dont on connaît la vérité terrain, ce qui permet de vérifier que la détection a raison.

## Sommaire

1. [Le problème](#le-problème)
2. [Architecture](#architecture)
3. [Technologies et justification](#technologies-et-justification)
4. [Règles de détection](#règles-de-détection)
5. [Lancer le projet](#lancer-le-projet)
6. [Scénario de démonstration](#scénario-de-démonstration)
7. [Vérifier les données Big Data](#vérifier-les-données-big-data)
8. [Tests](#tests)
9. [Organisation du dépôt](#organisation-du-dépôt)
10. [Répartition du travail](#répartition-du-travail)
11. [Limites assumées](#limites-assumées)
12. [Usage de l'IA générative](#usage-de-lia-générative)

## Le problème

En Tunisie, une part importante de l'eau produite est perdue dans les fuites, et les coupures ne sont souvent connues que lorsque les habitants se plaignent. Une fuite fait perdre de l'eau en continu : chaque minute gagnée sur la détection réduit les pertes.

AquaSentinel détecte l'anomalie en quelques secondes, indique **où** elle se trouve et **de quel type** elle est (fuite, coupure, qualité de l'eau), estime l'eau perdue, puis classe les secteurs les plus fragiles à partir de l'historique.

Mesures produites par chaque secteur, chaque seconde : pression (bar), débit (m³/h), niveau du réservoir (%) et turbidité (NTU).

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

- **Couche rapide** : Kafka → Spark Structured Streaming → MongoDB. Elle détecte les pannes en quelques secondes sur des fenêtres glissantes.
- **Couche batch** : Spark Streaming archive les mesures brutes sur HDFS (Parquet) ; un job Spark batch avec MLlib relit tout l'historique et calcule un classement des secteurs.
- **Couche de service** : MongoDB et l'API FastAPI. Elle est « simplifiée » : le dashboard lit deux collections sans les fusionner.

## Technologies et justification

| Brique | Rôle | Pourquoi ce choix |
|---|---|---|
| **Simulateur Python** | Pression, débit, réservoir, turbidité par secteur ; pannes à rampe progressive | Source contrôlée et reproductible, avec vérité terrain |
| **Kafka** (mode KRaft) | Collecte, tampon, découplage ; sert aussi de canal de commandes | Absorbe les pics, conserve et rejoue les messages ; si Spark tombe, les données ne sont pas perdues |
| **Spark Structured Streaming** | Fenêtres glissantes 20 s (pas de 10 s) par secteur, application des règles | Même API DataFrame que le batch, checkpoints, watermark pour les données tardives |
| **HDFS (Parquet)** | Archive brute partitionnée par date | Stockage distribué pour un volume croissant ; Parquet est colonnaire et compressé, donc efficace pour l'analytique |
| **Spark batch + MLlib** | Score de fragilité 0-100 et profils par KMeans | Traite tout l'historique ; le débit nocturne est l'indicateur classique des fuites dans un réseau d'eau |
| **MongoDB** | Base NoSQL de service | Documents flexibles, écritures rapides, upserts par secteur, index TTL pour l'historique court |
| **FastAPI + Leaflet + SSE** | Salle de contrôle en direct | Poussée temps réel vers le navigateur sans polling ; injection de pannes pour la démonstration |
| **Docker Compose** | Environnement | Un seul `docker compose up` lance les 9 services |

## Règles de détection

Définies dans `common/rules.py`, appliquées à la moyenne de chaque fenêtre glissante :

| État | Condition | Sévérité |
|---|---|---|
| **Coupure** | débit < 5 % du nominal **et** pression < 0,5 bar | 3 |
| **Fuite** | pression < 88 % du nominal **et** débit > 110 % du débit attendu à cette heure | 1, 2 ou 3 selon la chute de pression |
| **Qualité** | turbidité > 4 NTU | 2 jusqu'à 8 NTU, 3 au-delà |
| **Nominal** | aucune des conditions | 0 |

- Le débit « attendu » tient compte d'un profil de consommation sur 24 h (pointes le matin et le soir, creux la nuit), pour ne pas confondre une pointe de demande avec une fuite.
- La perte d'eau estimée est le débit mesuré moins le débit attendu.
- Les seuils sont des choix de conception, pas des normes officielles. Ils sont modifiables en haut de `common/rules.py`.

## Lancer le projet

**Prérequis** : Docker Desktop avec Docker Compose v2 (`docker compose version`) et environ 6 Go de RAM disponibles pour Docker. Une connexion Internet est nécessaire au premier démarrage. Ports utilisés : 8000, 9870, 27017 et 29092.

```bash
docker compose up --build
```

Le premier démarrage télécharge les images et les paquets Spark (plusieurs minutes). Ensuite :

| Interface | URL |
|---|---|
| **Dashboard AquaSentinel** | http://localhost:8000 |
| HDFS NameNode | http://localhost:9870 |

Pour arrêter : `docker compose down` (garde les données) ou `docker compose down -v` (efface les données ; recommandé pour repartir de zéro).

La carte utilise Leaflet et des tuiles OpenStreetMap chargées via Internet. Sans connexion, les secteurs, alertes et jauges fonctionnent, mais le fond de carte reste vide.

### Si HDFS pose problème

Remplacer `RAW_PATH` et `CHECKPOINT_RAW` par `file:///data/raw` et `file:///data/chk/raw` dans les services Spark, et monter un volume `./data:/data`. Le code est identique, seul le stockage change.

### Dépannage rapide

```bash
docker compose ps                       # état des 9 services
docker compose logs -f spark-streaming  # couche rapide (ou simulator, dashboard, spark-batch)
```

Le message `attente HDFS...` est normal pendant 1 à 2 minutes au démarrage.

## Scénario de démonstration

1. Ouvrir http://localhost:8000 : les 10 secteurs sont verts et l'heure simulée défile (un jour dure 4 minutes).
2. Choisir un secteur et cliquer sur **Fuite** : la pression chute, le secteur passe à l'orange puis au rouge, une alerte apparaît et le réservoir se vide. Un message affiche le **temps de détection** de bout en bout.
3. Essayer **Coupure** et **Contamination**. **Réparer** ramène le secteur à la normale.
4. Après quelques minutes, le panneau « Secteurs les plus fragiles » se remplit (job batch sur HDFS et KMeans). Pour un classement marqué, laisser une fuite ouverte quelques minutes sur un secteur.

Pour des pannes automatiques en démonstration continue, mettre `AUTO_FAULTS_EVERY: "40"` dans le service `simulator` de `docker-compose.yml`.

## Vérifier les données Big Data

| Brique | Commande ou lien |
|---|---|
| Kafka | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic water.sensors --max-messages 5` |
| HDFS (web) | http://localhost:9870 → *Utilities → Browse the file system* → `/aqua/raw` |
| HDFS (ligne de commande) | `docker compose exec namenode hdfs dfs -ls -R /aqua/raw` |
| MongoDB, états | `docker compose exec mongo mongosh aquasentinel --eval "db.sector_state.find().pretty()"` |
| MongoDB, alertes | `docker compose exec mongo mongosh aquasentinel --eval "db.alerts.find().sort({ts:-1}).limit(5).pretty()"` |
| MongoDB, classement batch | `docker compose exec mongo mongosh aquasentinel --eval "db.sector_ranking.find().sort({score:-1}).pretty()"` |
| Job batch | `docker compose logs spark-batch` → ligne `[batch] classement mis à jour` |

## Tests

Les tests ne nécessitent pas Docker. Utiliser **Python 3.10 ou 3.11** (PySpark 3.5.1 ne fonctionne pas avec Python 3.12 et suivants) et Java 11 ou 17.

```bash
pip install pytest mongomock fastapi httpx confluent-kafka pyspark==3.5.1 numpy
python -m pytest tests            # règles, simulateur, API du dashboard
python tests/spark_local_check.py # vrais jobs Spark en local (fichiers JSON à la place de Kafka)
```

- `tests/test_detection.py` : aucune fausse alerte sur 24 h pour les 10 secteurs, détection de chaque type de panne à plusieurs heures, retour à la normale après réparation.
- `tests/test_api.py` : API du dashboard sur une base simulée.
- `tests/spark_local_check.py` : `parse_events`, `aggregate`, `process_batch` et `batch_job` exécutés avec Spark local.

## Organisation du dépôt

```
common/      règles de détection, profil de demande, configuration des secteurs (partagé)
simulator/   modèle physique des secteurs, producteur Kafka, écoute des commandes
spark/       streaming_job.py (couche rapide), batch_job.py (couche batch + MLlib)
dashboard/   API FastAPI et interface Leaflet
tests/       tests unitaires et vérification Spark locale
docs/        déclaration d'usage de l'IA
```

## Répartition du travail

| Membre | Responsabilité | Fichiers principaux |
|---|---|---|
| **Aya Mhiri** | Ingestion : simulateur, Kafka, Docker Compose | `simulator/`, `docker-compose.yml`, `hadoop.env` |
| **Sarra Bouraoui** | Traitement temps réel : fenêtres, règles de détection, alertes, écriture dans MongoDB | `spark/streaming_job.py`, `common/` |
| **Ella Ibrahim** | Couche batch et restitution : HDFS, MLlib, dashboard | `spark/batch_job.py`, `spark/run_batch_loop.sh`, `dashboard/` |

Chaque membre du groupe maîtrise l'ensemble du pipeline, pas seulement sa partie.

## Limites assumées

- **Données simulées** : le modèle n'a ni topologie de réseau ni influence entre secteurs voisins, et le bruit des capteurs est gaussien.
- **Une seule machine** : un broker Kafka, un DataNode HDFS, Spark en mode `local[2]` et un MongoDB. L'architecture est pensée pour un cluster, mais la tolérance aux pannes n'est pas démontrée.
- **Volume modeste** : la démonstration produit environ 10 mesures par seconde. L'architecture est prévue pour monter en charge (partitions Kafka, Spark distribuable, HDFS, MongoDB), pas le jeu de données de la démonstration.
- **Écriture MongoDB via `collect()`** dans `process_batch` : correct pour 10 secteurs, à remplacer par une écriture parallèle pour un volume réel.
- **Petits fichiers Parquet** : un déclenchement toutes les 30 s produit de petits fichiers, qu'il faudrait compacter périodiquement.
- **Batch relancé toutes les 90 s** par un script, pour la démonstration. En production, un ordonnanceur (cron, Airflow) le lancerait, par exemple chaque nuit.
- **Aucune sécurité** : ni Kafka, ni MongoDB, ni le dashboard ne demandent d'authentification.
- **Secteur silencieux** : le dashboard l'affiche « hors ligne » après 20 s, mais aucune alerte n'est créée.

## Usage de l'IA générative

Une IA générative a été utilisée comme assistant technique pour le développement et le débogage. La déclaration d'usage signée par les membres du groupe se trouve dans `docs/declaration_usage_ia.md`.
