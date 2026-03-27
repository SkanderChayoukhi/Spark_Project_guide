# Guide Complet du Projet Big Data OpenSky

## 1) Pourquoi ce projet existe

Ce projet a pour but de construire un pipeline Big Data complet autour de l'API OpenSky (aviation), avec Kafka + Spark + stockage objet + visualisation.

En termes simples, vous avez construit une chaine qui fait:

1. collecte de donnees avion en temps reel;
2. mise en file dans Kafka;
3. traitement Spark en streaming (fenetres) et en batch;
4. jointure avec une source externe (OpenFlights);
5. persistance des resultats (Parquet puis Delta Lake, et MongoDB pour dashboard);
6. visualisation en dashboard.

## 2) Mapping exact avec les consignes de l'enseignant

### Exigence: 2 requetes batch (RDD + DataFrame)

- Fait dans le notebook N03 (`03_spark_batch_queries.ipynb`)
- RDD: comptage par continent via `map/filter/reduceByKey`
- DataFrame: aggregations altitude et volume par pays

### Exigence: 2 requetes streaming avec fenetre

- Fait dans N02 (`02_spark_streaming.ipynb`)
- Fenetre glissante: avions en vol par pays (2 min, slide 30s)
- Fenetre tumbling: vitesse moyenne/max par pays (1 min)

### Exigence: 1 requete SparkSQL

- Fait dans N03 (requete SQL sur vue `aircraft_data`)

### Exigence: 1 batch + graphique Pandas/Seaborn

- Fait dans N03 (visualisations Seaborn + export PNG)

### Exigence: jointure entre 2 sources

- Fait dans N04 (`04_spark_data_joins.ipynb`)
- Source A: MongoDB (resultats streaming)
- Source B: OpenFlights (CSV GitHub, aeroports)

### Exigence: streaming relie a un dashboard temps reel

- Fait dans N02 + app Streamlit
- N02 ecrit dans MongoDB via `foreachBatch`
- Streamlit lit MongoDB et affiche top pays + tableau

### Exigence: stockage objet Garage + Delta Lake

- Fait dans N05 (`05_delta_lake_writes.ipynb`)
- Conversion Parquet -> Delta
- Tables nommees + metadata + controle qualite + time travel
- Archive vers Garage (S3 compatible)

Conclusion: le projet couvre techniquement toutes les demandes principales.

## 3) Architecture reelle du depot

## Fichiers racine

- `README.md`: guide de lancement general (partiellement generique/ancien sur certains chemins)
- `AUDIT_CORRIGE.md`: audit de progression et validation fonctionnelle
- `requirements.txt`: dependances Python projet (Kafka, Spark, Delta, PyMongo, viz)
- `test_all_services.py`: script de verification des services Docker

## Dossiers

- `docker/docker-compose.yml`: infrastructure complete
- `src/`: scripts Python operationnels (producer + utilitaires)
- `notebooks/`: notebooks N01..N05 + test Kafka/Spark
- `config/`: exemples de config Spark et Garage
- `streamlit/`: app dashboard temps reel
- `data/`: sorties Parquet intermediaires
- `results/`: images/resultats exportes
- `docs/`: documentation complementaire

## 4) Infrastructure Docker service par service

Le fichier `docker/docker-compose.yml` orchestre:

1. Spark master (`spark`) + worker (`spark-worker`)
2. Jupyter notebook (`notebook`)
3. ZooKeeper (`zookeeper1`)
4. Kafka broker (`kafka1`)
5. Kafka UI (`kafka-ui`)
6. Garage (stockage S3) (`garage`)
7. Garage WebUI (`garage-webui`)
8. MongoDB (`mongodb`)
9. Grafana (`grafana`)
10. Streamlit (`streamlit`)

Points importants:

- Kafka expose deux listeners:
  - interne Docker: `kafka:9092`
  - externe host Windows: `localhost:9095`
- C'est crucial pour comprendre pourquoi le producer local utilise `9095` alors que les notebooks dans container utilisent souvent `kafka:9092`.

## 5) Explication code en profondeur: `src/producer_opensky.py`

Ce script est le point d'entree de la collecte en continu.

### Imports et constantes

- `requests`: appel HTTP vers OpenSky
- `KafkaProducer`: publication Kafka
- `datetime, timezone`: horodatage UTC d'ingestion
- Variables d'environnement:
  - `OPENSKY_URL` (defaut API publique)
  - `KAFKA_BOOTSTRAP` (defaut `localhost:9095`)
  - `KAFKA_TOPIC` (`opensky_raw`)
  - `POLL_INTERVAL` (defaut 30s)

### `STATE_FIELDS`

Liste des 17 attributs OpenSky dans l'ordre officiel de `states[]`.
Cette liste est critique car elle mappe les index tableau -> noms de colonnes.

### `create_producer()`

Configure un producer robuste:

- serialisation JSON UTF-8
- key serialisee en string (icao24)
- `acks='all'`: fiabilite (broker confirme ecriture)
- `retries=3`: resilience
- limites taille/message (`max_request_size`)
- micro-batching (`linger_ms`, `batch_size`)

### `fetch_opensky_states()`

- execute `GET` avec timeout 15s
- `raise_for_status()` pour gerer HTTP errors
- retourne JSON en cas succes
- retourne `None` si erreur reseau/API

### `parse_state_vector(state_array, batch_ts, batch_id)`

Transforme un tableau OpenSky en dictionnaire normalise:

1. boucle sur `STATE_FIELDS`
2. mappe chaque index s'il existe, sinon `None`
3. nettoie `callsign` (strip)
4. ajoute metadonnees:
   - `batch_id`
   - `batch_timestamp`
   - `ingestion_time` en ISO UTC

### `main()`

Boucle infinie:

1. recupere snapshot OpenSky
2. incremente numero de batch
3. pour chaque avion (`state_array`):
   - parse record
   - key = `icao24`
   - `producer.send(...)`
4. `producer.flush()` pour vidanger le buffer
5. log du batch
6. `sleep(POLL_INTERVAL)`

Gestion d'erreur:

- `KeyboardInterrupt` -> arret propre
- exceptions d'envoi: compte et affiche les 3 premieres
- exception globale: log + continue

Resultat: 1 message Kafka par avion, par cycle.

## 6) Utilitaires tests et observabilite

## `src/check_size.py`

Utilitaire ponctuel:

- appelle OpenSky
- construit un payload JSON
- calcule volume en MB

Utilite: estimer pression Kafka et taille moyenne des messages.

## `test_all_services.py`

Script de smoke-test infra:

- verifie HTTP: Kafka UI, Spark UI, Jupyter, Grafana, Garage WebUI
- verifie backend: Kafka (connexion + creation topic), MongoDB ping
- affiche bilan final OK/FAILED

Ce script est utile avant soutenance pour valider l'environnement en 1 commande.

## 7) Dashboard Streamlit (`streamlit/app.py`)

### Role

Lire les aggregations du streaming dans MongoDB et afficher un mini-dashboard temps reel.

### Flux interne

1. connexion MongoDB (`mongodb:27017`)
2. lecture des 1000 docs les plus recents (`sort _inserted_at desc`)
3. conversion en DataFrame Pandas
4. affichage:
   - metric total records
   - metric nb pays
   - metric total avions
5. graph bar top 10 pays (Plotly)
6. tableau detail 50 lignes
7. rerun auto

### Points de vigilance

- `st.rerun()` direct en bas peut entrainer boucle tres agressive selon version Streamlit
- l'auto-refresh est annonce a 30s dans le texte mais le code ne fait pas de pause explicite
- dans la pratique, preferer `st_autorefresh` ou logique temporelle conditionnelle

## Docker Streamlit

- image Python slim
- install `streamlit`, `pymongo`, `pandas`, `plotly`
- expose 8501

## 8) Configurations examples

## `config/spark_config.example.yaml`

Contient:

- infos Spark app/master
- endpoint S3A Garage
- chemins bronze/silver Delta
- Kafka bootstrap/topic
- MongoDB cible

Usage attendu: copie vers un fichier reel non versionne puis adaptation credentials.

## `config/garage_config.example.yaml`

Contient endpoint/admin key/bucket/access/secret.
Attention: credentials d'exemple presentes en clair -> a remplacer/retirer pour livraison publique.

## 9) Notebook N01: test API OpenSky

Objectif: comprendre la structure brute avant Kafka/Spark.

Cellules principales:

1. import libs + URL
2. appel API + statut + nombre d'avions
3. documentation des 17 champs
4. exemple 1 avion (indexes)
5. conversion DataFrame
6. stats descriptives
7. top pays
8. valeurs manquantes
9. graphique Seaborn
10. estimation taille JSON

Apport pedagogique: valide schema metier et contraintes rate-limit.

## 10) Notebook N02: streaming (vue globale)

Le detail approfondi est dans le second document, mais globalement N02 fait:

1. creation SparkSession avec package Kafka + Delta
2. lecture stream Kafka + parse JSON selon schema strict
3. requete fenetre glissante (avions en vol/pays)
4. requete fenetre tumbling (vitesse/pays)
5. ecriture MongoDB via `foreachBatch`
6. monitoring de requetes actives
7. arret propre des streams

Particularite observee:

- une cellule de demarrage de Query1 est dupliquee (redondance non bloquante mais a signaler en oral)

## 11) Notebook N03: batch (RDD + DataFrame + SQL)

Ce notebook couvre la partie batch et visualisation.

### Chargement

- lit MongoDB via PyMongo
- convertit vers Pandas puis Spark DataFrame

### Requete RDD

- map pays -> continent (gros dictionnaire)
- filtre `Other`
- aggregation `reduceByKey`
- tri decroissant

### Requete DataFrame

- groupBy pays
- aggregations sur `aircraft_count` et altitude
- top 10 altitude moyenne

### Requete SparkSQL

- vue temp `aircraft_data`
- requete SQL avec `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`

### Visualisations

- 3 graphiques comparatifs
- sauvegarde PNG dans `results/`

### Exports

- export Parquet:
  - `data/batch_results_rdd`
  - `data/batch_results_dataframe`
  - `data/batch_results_sql`

## 12) Notebook N04: jointures (vue globale)

N04 realise l'integration de deux sources:

- source 1: aggregats aircraft depuis MongoDB
- source 2: dataset aeroports OpenFlights (GitHub)

Strategie technique forte:

- aggregation prealable des 2 cotes par pays avant join
- evite l'explosion combinatoire due a cardinalite multiple aeroports/pays

Sorties:

- DataFrame enrichi par pays
- aggregation regionale par aeroport
- exports Parquet + CSV
- graphique de preuve

## 13) Notebook N05: Delta Lake et data management

N05 transforme les outputs N03/N04 en tables Delta.

Etapes:

1. setup Spark avec extensions Delta
2. smoke test Delta write/read
3. chargement Parquet N03/N04
4. enrichissement metadata (`created_at`, `version`, `source`, `row_count_snapshot`)
5. ecriture format Delta
6. creation tables nommees SQL
7. quality checks (row count, schema, nulls)
8. demo time travel (`versionAsOf`)
9. archive vers Garage via MinIO
10. export metadata JSON

Remarque securite:

- credentials Garage sont hardcodes dans une cellule (a neutraliser avant diffusion publique)

## 14) Script de test notebook: `notebooks/test_spark_kafka.py`

Petit test de connectivite Spark -> Kafka:

- cree SparkSession
- lit topic `opensky_raw` en batch (`earliest` -> `latest`)
- affiche nombre de messages lus

But: verifier rapidement que Spark peut consommer Kafka.

## 15) Flux de donnees bout en bout

1. OpenSky API renvoie snapshot global avions
2. producer transforme chaque avion en record JSON
3. producer envoie records dans Kafka topic `opensky_raw`
4. Spark Streaming lit Kafka et parse JSON
5. requetes fenetrees calculent aggregats pays
6. `foreachBatch` ecrit aggregats dans MongoDB
7. Streamlit lit MongoDB et affiche dashboard temps reel
8. batch notebooks exploitent MongoDB pour analyses complementaires
9. N04 joint avec OpenFlights pour enrichissement geoposition
10. N05 convertit resultats en Delta et archive Garage

## 16) Ce que vous pouvez dire en soutenance (synthese claire)

- Le projet implemente un pipeline lambda simplifie: streaming + batch.
- Kafka decouple ingestion API et traitement Spark.
- Le streaming calcule des KPI dynamiques par fenetre temporelle.
- Le batch apporte analyses plus lourdes et visualisations.
- La jointure OpenFlights enrichit la valeur metier des donnees.
- Delta Lake apporte ACID, historique et fiabilite data lake.

## 17) Risques/limites a connaitre (rigueur orale)

1. Rate limit OpenSky (anonyme) limite la frequence possible.
2. Certaines configurations sont heterogenes (host `localhost:9095` vs docker `kafka:9092`).
3. Quelques credentials en clair dans des examples/notebooks.
4. `foreachBatch` avec `count()` puis `collect()` peut etre couteux a grande echelle.
5. La jointure est au niveau pays (pas vol individuel), donc precision aeroport/vol limitee.

## 18) Classement des 5 thematiques d'approfondissement

Classement selon integration dans ce projet:

1. Delta Lake (Data Lake) -> integre en profondeur (N05)
2. Formats de donnees / schema (JSON, Parquet, Delta) -> integre
3. MLLib / Spark ML -> non integre
4. GraphX (graphes) -> non integre
5. Spark NLP -> non integre

Votre groupe a donc bien choisi et implemente la thematique Delta Lake.

## 19) Check-list de reproduction rapide

1. `docker compose up -d` depuis `docker/`
2. verifier services avec `python test_all_services.py`
3. lancer producer: `python src/producer_opensky.py`
4. executer N02 pour peupler MongoDB
5. executer N03 puis N04
6. executer N05 pour Delta
7. ouvrir Streamlit (`localhost:8501`) et Kafka UI (`localhost:8082`)

## 20) Conclusion

Le projet est coherent, complet et pedagogiquement solide:

- ingestion temps reel fiable,
- traitements streaming et batch conformes,
- jointure multi-source reelle,
- visualisation,
- stockage moderne Delta Lake,
- environnement reproductible Docker.

Pour l'oral, insistez sur les choix d'architecture (decouplage Kafka, fenetres Spark, pre-aggregation avant join, conversion Delta) et sur les compromis techniques (cout des operations, granularite des donnees, securite des credentials).
