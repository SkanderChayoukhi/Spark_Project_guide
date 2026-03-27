# Guide Focus Presentation: Streaming/Fenetres (N02) et Jointures (N04)

Ce document est fait pour quelqu'un qui part de zero.
Objectif: vous donner une comprehension technique precise de ce que fait chaque cellule, pourquoi elle existe, et comment l'expliquer proprement a l'oral.

## Partie A - Notebook N02: `02_spark_streaming.ipynb`

## 1) Idee metier de N02

On recoit un flux continu d'avions depuis Kafka.
On veut calculer en continu:

1. combien d'avions en vol il y a par pays (fenetre glissante),
2. la vitesse moyenne/max par pays (fenetre tumbling),
3. puis envoyer ces resultats vers MongoDB pour dashboard.

## 2) Cellule 1 - Titre et pre-requis

Le markdown de debut annonce clairement:

- 2 requetes streaming avec fenetres,
- dependances runtime (docker up, producer lance, Java OK).

A dire a l'oral:

- Spark Structured Streaming fonctionne en micro-batches;
- chaque micro-batch lit les nouveaux messages Kafka, applique transformations, puis publie vers un sink.

## 3) Cellule SparkSession - pourquoi chaque ligne compte

Code cle:

- `PYSPARK_SUBMIT_ARGS` avec packages:
  - `spark-sql-kafka`: connecteur Kafka
  - `delta-spark`: support Delta
- Builder:
  - `appName`: nom du job
  - `master("local[*]")`: execution locale multi-coeurs
  - `spark.sql.extensions` + `catalog`: active Delta
  - `forceDeleteTempCheckpointLocation`: evite accumulation de checkpoints temporaires
  - memoire driver 2G

Ce que ca implique:

- sans package Kafka, `format("kafka")` echoue.
- meme si Delta n'est pas ecrit dans N02, la session est deja preparee pour compatibilite globale du projet.

## 4) Cellule schema + lecture Kafka

### 4.1 Construction du schema

`StructType([...])` decrit exactement le JSON emis par `producer_opensky.py`.

Pourquoi schema explicite important:

- parsing deterministe;
- types robustes (`DoubleType`, `BooleanType`, `LongType`...);
- evite erreurs de schema infere et facilite optimisations Catalyst.

### 4.2 Lecture stream Kafka

Options principales:

- `kafka.bootstrap.servers`: endpoint broker
- `subscribe`: topic `opensky_raw`
- `startingOffsets = latest`: demarre au dernier offset disponible
- `failOnDataLoss = false`: tolerance en cas retention/changement offsets
- timeouts/reconnect: robustesse reseau

Important oral:

- `latest` = on suit le flux a partir de maintenant (pas replay historique).
- pour debug ou reprocessing, on pourrait choisir `earliest`.

### 4.3 Parsing JSON

Pipeline:

1. cast `value` Kafka en string
2. `from_json(..., schema)` -> struct `data`
3. `select("data.*")` -> aplatissement
4. creation `event_time` via `to_timestamp(ingestion_time)`

`event_time` sert de reference temporelle pour les fenetres et watermark.

## 5) Requete 1 - Fenetre glissante (sliding)

### 5.1 Objectif

Compter les avions en vol par pays avec:

- taille fenetre 2 minutes,
- pas 30 secondes,
- watermark 1 minute.

### 5.2 Lecture technique de la transformation

Pipeline logique:

1. `filter(on_ground == False)`
2. `withWatermark("event_time", "1 minute")`
3. `groupBy(window(event_time, "2 minutes", "30 seconds"), origin_country)`
4. `agg(count(*), avg(baro_altitude))`
5. projection des colonnes finales

### 5.3 Comment interpreter la fenetre glissante

A l'instant t, un event peut appartenir a plusieurs fenetres qui se chevauchent.

Exemple:

- fenetre A: 10:00 -> 10:02
- fenetre B: 10:00:30 -> 10:02:30

Un event a 10:01 est compte dans A et B.

Interet:

- meilleure granularite temporelle (mise a jour frequente)
- courbe plus lisse pour monitoring.

### 5.4 Watermark: role exact

`withWatermark("event_time", "1 minute")` signifie:

- Spark accepte les donnees en retard jusqu'a 1 minute;
- au-dela, elles peuvent etre ignorees pour fermer l'etat des fenetres;
- cela limite la croissance memoire de l'etat streaming.

## 6) Demarrage Query 1 en sink memory

Cellule de start:

1. boucle sur `spark.streams.active`
2. stop ancienne query de meme nom
3. `writeStream.format("memory")`
4. `outputMode("update")`
5. `queryName("aircraft_by_country")`
6. trigger toutes les 10 secondes

Utilite du sink memory:

- pratique en notebook pour interroger les resultats via Spark SQL.

`outputMode("update")`:

- seules les lignes modifiees dans l'etat sont republiees.

## 7) Cellule attente + affichage SQL

- pause 30s pour laisser arriver des micro-batches
- requete SQL sur table memoire `aircraft_by_country`
- tri desc par `aircraft_count`

Interpretation:

- c'est un snapshot analytique du streaming state a l'instant ou la cellule est executee.

## 8) Redondance observee: cellule Query1 dupliquee

Le notebook contient une deuxieme cellule identique de demarrage Query1.

Impact:

- non bloquant si l'ancienne query est arretee avant restart,
- mais redondant et potentiellement confus en demo.

Message oral possible:

- oui, il y a redondance de cellule de start, garde-fou present (stop query active), impact fonctionnel limite.

## 9) Requete 2 - Fenetre tumbling

### 9.1 Objectif

Calculer vitesses par pays:

- moyenne km/h,
- max km/h,
- volume avions.

Fenetre tumbling 1 minute, sans recouvrement.

### 9.2 Pipeline

1. filtre avions en vol + vitesse non nulle
2. watermark 1 minute
3. groupBy `window(event_time, "1 minute")` + pays
4. agregats:
   - `avg(velocity) * 3.6`
   - `max(velocity) * 3.6`
   - `count(*)`
5. selection colonnes finales

### 9.3 Tumbling vs sliding

- tumbling: fenetres disjointes, chaque event appartient a une seule fenetre
- sliding: fenetres chevauchantes, event potentiellement multi-fenetres

Usage metier:

- tumbling = KPI periodique stable (par minute)
- sliding = suivi plus reactif et continu

## 10) Demarrage Query2 + lecture

Meme pattern que Query1:

- stop ancienne query homonyme
- sink memory `velocity_by_country`
- trigger 10s
- affichage SQL top vitesses

A noter:

- resultats vitesse dependent de la qualite des valeurs `velocity` issues OpenSky.

## 11) Ecriture MongoDB via `foreachBatch`

## 11.1 Variables Mongo

- URI par defaut: `mongodb://mongodb:27017/`
- DB: `opensky_dashboard`

## 11.2 Fonction `write_to_mongodb(batch_df, batch_id)`

Logique:

1. si batch vide -> return
2. `collect()` vers liste Python
3. conversion fields timestamp
4. ajout metadata:
   - `_batch_id`
   - `_inserted_at` UTC
5. `insert_many(records)` dans collection `aircraft_by_country`

Pourquoi `foreachBatch` est puissant:

- on garde API streaming Spark tout en utilisant un connecteur externe non natif Spark (PyMongo).

Limite technique:

- `count()` + `collect()` peut etre lourd si gros volume.
- convient bien ici car sortie deja agregee par fenetre/pays.

## 12) Monitoring et arret des streams

### Monitoring

Affiche pour chaque query active:

- nom/id/status
- `numInputRows`
- `batchDuration`

C'est la preuve runtime que le pipeline tourne reellement.

### Arret

Boucle de stop sur toutes les queries active.
Essentiel pour eviter jobs orphelins et etats verrouilles.

## 13) Resume ultra-court a presenter pour N02

- Kafka fournit flux brut par avion.
- Spark parse schema et cree `event_time`.
- Query1 (sliding 2min/30s): volume avion en vol par pays.
- Query2 (tumbling 1min): vitesses moyenne/max par pays.
- Watermark 1min: gestion retard + maitrise memoire.
- `foreachBatch` pousse vers MongoDB pour dashboard live.

## Partie B - Notebook N04: `04_spark_data_joins.ipynb`

## 1) Idee metier de N04

Fusionner:

1. activite aerienne agr egee (source streaming historisee MongoDB),
2. referentiel aeroportuaire mondial OpenFlights,

pour obtenir une vue enrichie par pays/aeroport.

## 2) Setup Spark

Comme N03:

- stop session precedente
- setup packages Spark/Kafka/Delta
- Spark local[2], memoire 1G/1G, shuffle partitions 4

But:

- environnement controle et reproductible en notebook.

## 3) Chargement source 1: MongoDB

Pipeline:

1. connexion MongoDB + ping
2. lecture `opensky_dashboard.aircraft_by_country`
3. conversion liste docs -> Pandas
4. suppression `_id`
5. conversion Pandas -> Spark DataFrame `df_aircraft`

Raison de ce choix:

- simple a mettre en oeuvre sans connecteur Spark Mongo specifique.

## 4) Chargement source 2: OpenFlights GitHub

### 4.1 Telechargement

- URL brute GitHub `airports.dat`
- `requests.get()` + `raise_for_status()`

### 4.2 Parsing CSV

- `pd.read_csv(..., header=None, names=[...])`
- `na_values=['\\N']` pour convertir valeurs manquantes

### 4.3 Nettoyage

1. conversion latitude/longitude en numerique
2. conservation colonnes utiles
3. filtre pays + coordonnees non nulles
4. suppression doublons sur `(iata_code, country)`
5. conversion vers Spark `df_airports`

Impact metier:

- vous utilisez une vraie base aeroport mondiale, pas un mock.

## 5) Preparation join

- verification colonnes des 2 DataFrames
- renommage `origin_country` -> `country` pour aligner la cle

C'est l'etape de normalisation des schemas avant jointure.

## 6) Join inner: pourquoi pre-aggregation est indispensable

### 6.1 Risque sans pre-aggregation

Si vous joignez directement:

- plusieurs lignes aircraft par pays
- plusieurs aeroports par pays

vous obtenez un produit cartesian partiel par pays (explosion combinatoire).

### 6.2 Strategie appliquee

- `df_aircraft_agg = groupBy(country).agg(...)`
- `df_airports_dedup = groupBy(country).agg(first(...), count(*))`
- puis `inner join on country`

Resultat:

- 1 ligne/pays cote aircraft
- 1 ligne/pays cote airports
- join stable et interpretable.

### 6.3 Aggregats aircraft

Vous calculez:

- moyenne, min, max de `aircraft_count`
- somme totale `total_aircraft_count`
- moyenne altitude

Donc le join transporte deja des indicateurs business.

### 6.4 Aggregats airports

`first(...)` prend un aeroport representatif du pays.
`count(*)` donne nombre d'aeroports disponibles.

Limite a connaitre:

- `first` sans ordre impose peut varier selon execution.
- acceptable pour demo, mais en prod on choisirait une regle deterministe (aeroport principal, plus traffic, etc.).

## 7) Left join

Objectif:

- conserver tous les pays aircraft, meme sans match aeroport.

Implementation:

- meme pre-aggregation airports
- `how="left"`
- comptage `iata_code is null` pour mesurer couverture referentiel.

Valeur analytique:

- quantifie les trous de donnees de la source externe.

## 8) Enrichissement business

DataFrame `df_enriched` construit:

- colonnes pays/aeroport
- metriques traffic
- coordonnees geo
- `aircraft_thousands = total / 1000`
- `traffic_category`:
  - High si >100
  - Medium si >50
  - sinon Low
- `airport_id = country-iata`

Ce bloc transforme une jointure technique en jeu de donnees interpretable.

## 9) Aggregation post-join

`df_regional = groupBy(iata_code, airport_name, country).agg(...)`

Sorties:

- avg aircraft
- total aircraft
- avg altitude
- nb observations

Tri par `total_aircraft` decroissant.

Cela donne un classement par aeroport/pays exploitable en reporting.

## 10) Visualisation de preuve

- creation dossier `results`
- top 10 pays par `num_airports_in_country`
- barh matplotlib
- sauvegarde `joins_simple.png`

Ce graphique prouve rapidement que la jointure a produit un jeu coher ent.

## 11) Exports finaux N04

- Parquet enrichi: `data/join_aircraft_airports`
- Parquet aggregation regionale: `data/join_regional_agg`
- CSV: `results/joins_enriched.csv`

Ces exports alimentent N05 (Delta).

## 12) Resume ultra-court a presenter pour N04

- Source A: activite avion issue du streaming (MongoDB)
- Source B: referentiel aeroports OpenFlights (GitHub)
- Nettoyage schema + normalisation cle `country`
- Pre-aggregation des 2 cotes pour eviter explosion combinatoire
- Inner join pour donnees communes + left join pour couverture
- Enrichissement KPI (`traffic_category`, `airport_id`)
- Export Parquet/CSV pour exploitation Delta et reporting

## Partie C - Questions de jury probables + reponses

## Q1: Pourquoi utiliser watermark en streaming?

Parce que les donnees peuvent arriver en retard. Watermark fixe une tolerance temporelle et permet a Spark de fermer proprement l'etat des fenetres pour maitriser memoire et latence.

## Q2: Pourquoi deux types de fenetres?

- Sliding: vision continue, updates frequentes.
- Tumbling: buckets disjoints, lecture periodique simple.

Montrer les deux prouve maitrise des paradigmes temporels Spark.

## Q3: Pourquoi pre-aggregation avant join?

Pour eviter multiplication artificielle des lignes due a cardinalites multiples sur la meme cle de jointure.

## Q4: Pourquoi MongoDB entre streaming et dashboard?

MongoDB sert de store temps reel simple pour lecture UI, decouple Spark et dashboard, et evite de faire lire Kafka directement par front.

## Q5: Quelle limite principale des jointures actuelles?

Jointure au niveau pays, pas vol individuel ni aeroport de depart/arrivee reel. C'est un enrichissement macro, pas une attribution aeroport exacte par vol.

## Partie D - Script oral 2 minutes (pret a dire)

Dans N02, on lit en continu les messages OpenSky depuis Kafka, on parse un schema strict puis on construit deux agregations fenetrees. La premiere est une fenetre glissante 2 minutes avec pas 30 secondes pour suivre finement le nombre d'avions en vol par pays. La deuxieme est une fenetre tumbling 1 minute pour calculer vitesse moyenne et maximale par pays sans recouvrement. On ajoute un watermark de 1 minute pour gerer les retards et limiter l'etat memoire. Enfin, on envoie les resultats vers MongoDB avec foreachBatch, ce qui alimente le dashboard temps reel.

Dans N04, on integre une deuxieme source externe, OpenFlights, pour enrichir les resultats aircraft. On nettoie les schemas et on joint sur le pays. Avant la jointure, on agrege chaque source par pays pour eviter l'explosion combinatoire. On produit un dataset enrichi avec metriques traffic, categorie de trafic, identifiant aeroport, puis on exporte en Parquet et CSV. Cette sortie est ensuite reutilisee dans N05 pour la couche Delta Lake.

## Partie E - Check-list avant votre presentation

1. Verifier que producer tourne et Kafka recoit des messages.
2. Montrer dans N02 qu'il y a des queries actives (`spark.streams.active`).
3. Montrer un extrait MongoDB alimente par `foreachBatch`.
4. Expliquer clairement sliding vs tumbling avec un mini schema temporel oral.
5. Dans N04, insister sur pre-aggregation avant join.
6. Montrer les fichiers exportes Parquet/CSV a la fin.
7. Garder en tete les limites (join macro par pays, credentials a securiser).
