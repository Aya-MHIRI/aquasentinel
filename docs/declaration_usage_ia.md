# Déclaration d'usage de l'IA générative

Projet Big Data (II3) : AquaSentinel

> Modèle à compléter **honnêtement** par chaque membre du groupe, puis à imprimer et signer.
> Cadre rappelé par l'énoncé : usage responsable (Zone 3 de la charte), pour assister techniquement
> le développement et le débogage, **pas pour remplacer l'étudiant**.

**Outil(s) utilisé(s)** : ChatGPT , Claude  **Période** : 22/10/2026 - 03/11/2026

## Ce pour quoi l'IA a été utilisée (cocher et préciser)

- [x] Idée / choix du sujet : On a travaillé sur la surveillance des fuites, coupures et anomalies de qualité de l'eau dans dix secteurs simulés du Grand Tunis. ChatGPT a aidé à structurer cette idée autour d'une architecture avec une couche temps réel et une couche historique.
- [x] Génération d'un squelette de code (docker-compose, simulateur, jobs Spark, dashboard) : On a principalement utilisé **Claude pour générer et structurer le code initial** du simulateur Python, de l'ingestion Kafka, des traitements Spark, du stockage HDFS/MongoDB et du dashboard FastAPI/Leaflet. ChatGPT a également été utilisé comme assistant technique.
- [x] Débogage / explication d'erreurs : L'IA nous a aidées à proposer des tests de détection et des méthodes de vérification des traitements Spark en local.
- [x] Rédaction ou relecture de la documentation / des diapos : On a utilisé ChatGPT et Claude pour nous aider à rédiger le README, organiser les diapositives et reformuler certaines explications techniques.

## Ce que nous avons fait nous-mêmes

Décrire ce que le groupe a compris, adapté, testé et modifié (par exemple : seuils des règles, choix des fenêtres, scénarios de démo, corrections) :

On a travaillé sur l'intégration et la validation fonctionnelle des différentes briques du pipeline Big Data. On a étudié les échanges entre les capteurs simulés, les topics Kafka, les traitements Spark et MongoDB, ainsi que la cohérence des règles de détection basées sur la pression, le débit et la turbidité.

On a également examiné le traitement par fenêtres glissantes, l'archivage des données sur HDFS au format Parquet et le calcul des scores de fragilité avec Spark MLlib. Pour la validation, on a préparé des scénarios d'injection de fuites, de coupures et de contaminations, puis analysé les alertes, les changements d'état des secteurs et les temps de détection affichés sur le dashboard.

Enfin, on a identifié les limites techniques du prototype, notamment l'utilisation de capteurs simulés et l'exécution de Spark en mode local, afin de pouvoir justifier nos choix lors de la soutenance.


## Engagement

Nous déclarons comprendre l'ensemble du code rendu et être en mesure de l'expliquer lors de la soutenance.

| Nom et prénom | Date | Signature |
|:----------------|:------------:|:----------------:|
| Sarra Bouraoui | 03/11/2026 | |
| Ella Ibrahim | 03/11/2026 | |
| Aya Mhiri | 03/11/2026 | |
