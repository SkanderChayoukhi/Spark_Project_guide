# Presentation orale 10 minutes - Ta partie

Sujet de ta partie: Streaming avec fenetres (N02) + Jointures (N04)

Objectif de ce document: te donner un script pret a dire, avec timing, contenu slide, transition orale, demo live, et reponses aux questions probables.

## 0) Strategie de prise de parole

Tu dois paraitre:

1. maitriser le pourquoi metier,
2. maitriser le comment technique,
3. maitriser les limites et compromis.

Regle d'or:

- une idee forte par slide,
- annoncer ce que tu vas dire,
- montrer une preuve (code/resultat),
- conclure en 1 phrase d'impact.

## 1) Plan minute par minute (10:00)

- 00:00 -> 00:45: introduction de ta partie
- 00:45 -> 04:30: N02 streaming fenetres
- 04:30 -> 08:00: N04 jointures
- 08:00 -> 09:15: demo live rapide
- 09:15 -> 10:00: conclusion + ouverture questions

## 2) Slides recommandees (7 slides)

## Slide 1 - Perimetre de ma partie (45s)

Titre propose:
Streaming temps reel et enrichissement par jointure

A afficher:

- Source flux: OpenSky -> Kafka
- Traitement: Spark Structured Streaming (N02)
- Enrichissement: Spark joins avec OpenFlights (N04)
- Sorties: MongoDB dashboard + Parquet pour Delta

Script exact a dire:

Dans ma partie, je vais montrer deux briques centrales du projet. D'abord le traitement streaming avec fenetres temporelles pour produire des indicateurs en quasi temps reel. Ensuite la jointure entre nos resultats aircraft et une base aeroportuaire mondiale OpenFlights pour enrichir la valeur metier des donnees. Je vais presenter le principe, le code, puis une demo courte.

Transition:

Je commence par le streaming et la logique des fenetres.

## Slide 2 - Architecture N02 (1 min)

A afficher:

OpenSky API -> producer_opensky.py -> Kafka topic opensky_raw -> Spark N02 -> MongoDB -> Streamlit

Message cle:

- micro-batches Spark sur un flux continu Kafka
- event_time derive de ingestion_time

Script exact a dire:

Le producer envoie un message Kafka par avion toutes les 30 secondes. Dans N02, Spark lit ce flux avec Structured Streaming, parse un schema strict, puis cree un event_time qui sert de reference temporelle. Ensuite, on calcule deux agregations fenetrees complementaires. Les resultats sont ecrits dans MongoDB via foreachBatch pour alimenter le dashboard.

Phrase de preuve:

Le pipeline est observable via spark.streams.active et les compteurs de micro-batch.

## Slide 3 - Fenetre glissante N02 Query 1 (1 min 45)

A afficher:

- objectif: avions en vol par pays
- window: 2 minutes
- slide: 30 secondes
- watermark: 1 minute
- output mode: update

Mini schema temporel:

- fenetre A: 10:00 -> 10:02
- fenetre B: 10:00:30 -> 10:02:30

Script exact a dire:

La premiere requete compte les avions en vol par pays dans une fenetre glissante. Une fenetre de deux minutes est recalculee toutes les trente secondes, donc les fenetres se chevauchent. Un meme evenement peut contribuer a plusieurs fenetres. C'est utile pour un suivi reactif et lisse. On applique aussi un watermark de une minute pour gerer les donnees retardataires et limiter la croissance de l'etat en memoire.

Point technique fort a citer:

La transformation est filter sur on_ground false, puis groupBy window event_time 2 minutes 30 secondes, puis agg count et moyenne altitude.

Transition:

La deuxieme requete utilise une autre logique temporelle.

## Slide 4 - Fenetre tumbling N02 Query 2 (1 min)

A afficher:

- objectif: vitesse moyenne et max par pays
- window tumbling: 1 minute
- pas de recouvrement
- conversion m/s -> km/h

Script exact a dire:

La deuxieme requete calcule la vitesse moyenne et maximale par pays dans des fenetres tumbling de une minute. Ici il n'y a pas de recouvrement: chaque evenement appartient a une seule fenetre. C'est plus simple a lire pour des KPI periodiques. On convertit les vitesses de metres par seconde en kilometres par heure avec un facteur 3.6.

Conclusion de slide:

Donc N02 combine deux visions temporelles: react ive avec sliding, et periodique stable avec tumbling.

## Slide 5 - Ecriture MongoDB et monitoring (45s)

A afficher:

- foreachBatch -> collection aircraft_by_country
- metadonnees batch: \_batch_id, \_inserted_at
- monitoring: numInputRows, batchDuration

Script exact a dire:

Pour brancher le dashboard, on utilise foreachBatch. Chaque micro-batch est converti et insere dans MongoDB avec des metadonnees de lot. Cela permet une lecture simple cote Streamlit et une tracabilite minimale. On suit aussi les indicateurs de run comme le nombre de lignes traitees et la duree de batch pour verifier que la chaine est saine.

Transition:

Je passe maintenant a la deuxieme partie: les jointures N04.

## Slide 6 - Jointures N04 et strategie anti explosion (2 min)

A afficher:

- Source A: MongoDB aircraft_by_country
- Source B: OpenFlights airports.dat GitHub
- Cle de join: country
- pre-aggregation des 2 cotes
- inner join + left join
- enrichissements: traffic_category, airport_id

Script exact a dire:

Dans N04, on fusionne nos resultats aircraft avec une base aeroportuaire mondiale reelle, OpenFlights. Le risque principal d'une jointure naive est l'explosion combinatoire, car on peut avoir plusieurs lignes aircraft par pays et plusieurs aeroports par pays. Pour eviter ce probleme, on agrege d'abord les deux sources par pays, puis on joint. On fait un inner join pour les pays communs, et un left join pour mesurer les pays sans correspondance aeroportuaire.

Puis on enrichit les donnees avec des indicateurs business, notamment une categorie de trafic High Medium Low selon le volume total, et un identifiant airport_id.

Message d'expert a dire:

Ce choix privilegie la stabilite analytique et l'interpretabilite, au prix d'une granularite plus macro au niveau pays.

## Slide 7 - Resultats, limites, valeur (1 min 45)

A afficher:

- output N04: join_aircraft_airports parquet, join_regional_agg parquet, joins_enriched.csv
- valeur: enrichissement geoposition + comparabilite pays
- limites: join au niveau pays, pas vol unitaire
- integration suite: N05 Delta Lake

Script exact a dire:

Les resultats sont exportes en Parquet et CSV, puis reutilises dans N05 pour la couche Delta Lake. La valeur apportee est claire: on passe d'un flux avion brut a une lecture enrichie par contexte aeroportuaire. La limite assumee est que la jointure est faite au niveau pays et non au niveau trajectoire individuelle de vol. En revanche, cette approche reste robuste, explicable et suffisante pour les objectifs du projet et du dashboard.

Cloture:

Je vais finir avec une demo de verification en direct.

## 3) Demo live 1 minute 15 (script concret)

Ordre conseille:

1. Montrer N02 cellule monitoring des requetes actives
2. Montrer un SELECT top pays depuis la table memory aircraft_by_country
3. Montrer dans MongoDB que les documents arrivent
4. Montrer N04 resultat de join (show 10)

Script exact a dire:

Ici on voit les requetes streaming actives et leurs metriques de batch. Je lance maintenant la lecture du top pays en fenetre glissante. Ensuite je verifie que les lots sont bien ecrits dans MongoDB avec un inserted_at recent. Enfin je montre le resultat de jointure N04 avec les champs enrichis iata, latitude, longitude et la categorie de trafic.

Si la demo prend du retard:

- saute MongoDB detail
- montre directement monitoring + show de jointure

## 4) Questions du jury et reponses courtes

Q: Pourquoi watermark 1 minute?
R: Pour accepter un retard raisonnable tout en bornant l'etat streaming. Sans watermark, la memoire et la latence peuvent degrader.

Q: Pourquoi pas une jointure sans pre-aggregation?
R: Parce qu'on cree une multiplication artificielle des lignes due aux cardinalites multiples par pays. La pre-aggregation stabilise la jointure.

Q: Pourquoi MongoDB et pas lecture directe dashboard depuis Spark?
R: MongoDB sert de couche de restitution rapide et decouplee. Le dashboard lit une base simple sans dependre de l'execution Spark interactive.

Q: Quelle est la principale limite de votre join?
R: Le niveau de granularite est pays, pas vol individuel. C'est un enrichissement macro volontaire.

Q: Qu'est-ce qui prouve que ce n'est pas juste theorique?
R: On montre run actif, metriques micro-batch, insertion Mongo et outputs Parquet exploitables en aval.

## 5) Fiche anti stress (30 secondes avant de passer)

A relire juste avant de parler:

- je donne le pourquoi avant le code
- je compare sliding versus tumbling simplement
- je cite watermark et foreachBatch
- je justifie la pre-aggregation avant join
- je reconnais une limite sans me justifier trop longtemps
- je termine sur la valeur apportee et l'integration Delta

Phrase finale prete:

Ma partie montre qu'on sait passer d'un flux brut temps reel a des indicateurs fiables, puis a des donnees enrichies multi-sources exploitables pour l'analyse et le stockage Delta.

## 6) Version ultra courte si tu n'as que 3 minutes

1. N02 lit Kafka, parse schema et cree event_time.
2. Query sliding 2 min 30s: nb avions en vol par pays.
3. Query tumbling 1 min: vitesse moyenne et max par pays.
4. Watermark 1 min pour retard et maitrise d'etat.
5. foreachBatch ecrit MongoDB pour dashboard.
6. N04 joint Mongo aircraft avec OpenFlights airports.
7. Pre-aggregation avant join pour eviter explosion combinatoire.
8. Export Parquet/CSV puis integration Delta en N05.

Fin.
