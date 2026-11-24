# Flight Streaming Lakehouse

Suivi du trafic aérien en quasi temps réel sur Azure. Les positions des avions sont récupérées
en continu depuis l'API publique [OpenSky Network](https://opensky-network.org/), envoyées dans
Azure Event Hubs, puis traitées par Spark Structured Streaming sur Databricks dans un lakehouse
Delta (bronze / silver / gold) stocké sur ADLS Gen2.

Sur la zone Europe + Maghreb, un relevé contient environ 2 000 avions.

## Architecture

```
OpenSky API ──> producteur Python (Azure Container Instances)
                    │  JSON, partitionné par avion
                    ▼
               Azure Event Hubs  (endpoint Kafka)
                    │
                    ▼  Databricks Structured Streaming, trigger 1 min
   bronze.positions_raw          message brut + offset / partition / heure d'arrivée
                    │
                    ▼
   silver.positions              typage, règles qualité, enrichissement,
   silver.positions_quarantine   dédoublonnage dans le watermark (10 min)
                    │
                    ▼
   gold.airport_traffic_5min     avions autour de chaque aéroport, arrivées / départs / au sol
   gold.country_traffic_5min     avions en vol par pays d'immatriculation, vitesse et altitude moyennes
                    │
                    ▼
               Power BI (DirectQuery, rafraîchissement 1 min)
```

Toute l'infrastructure est décrite en Terraform (`infra/terraform`) et déployée par GitHub Actions.

## Points techniques

**Dédoublonnage.** OpenSky renvoie la dernière position connue de chaque avion : un avion qui n'a pas
émis depuis le dernier relevé réapparaît avec le même `time_position`. Silver supprime ces doublons
sur `(icao24, time_position)` avec `dropDuplicatesWithinWatermark`, ce qui garde un état borné
(10 minutes) au lieu de mémoriser toutes les clés depuis le démarrage.

**Données tardives.** Le temps de l'événement est `time_position` (heure GPS de la position), pas
l'heure d'arrivée dans Event Hubs. Les fenêtres de 5 minutes en gold sont fermées après un watermark
de 10 minutes et écrites en mode append : une fenêtre n'est écrite qu'une fois, quand elle est complète.

**Qualité.** Chaque message reçoit la liste des règles qu'il viole : JSON illisible, identifiant
ICAO invalide, position absente ou hors limites, altitude hors [-500 m ; 20 000 m], vitesse hors
[0 ; 400 m/s], horodatage absent. Les lignes valides vont en silver, les autres en quarantaine avec
la raison du rejet, rien n'est perdu.

**Aéroports.** Un avion est compté pour un aéroport s'il est à moins de 30 km et sous 3 000 m
(ou au sol). Un filtre par boîte de latitude / longitude est appliqué avant la distance de haversine
pour que la jointure avec la table des aéroports (diffusée en broadcast) reste légère. La phase
de vol (montée, descente, croisière, sol) vient du taux de montée.

**Une requête par couche.** Bronze, silver et gold sont des streams séparés avec leurs propres
checkpoints : on peut relancer ou retraiter une couche sans toucher aux autres, et bronze garde
toujours le message brut pour rejouer l'historique si une règle change.

**Partitionnement.** Le producteur envoie les positions d'un même avion sur la même partition
Event Hubs pour conserver leur ordre.

## Lancer en local

Prérequis : Python 3.10+ et Java 17 ou 21.

```bash
pip install -e ".[dev]"
pytest -q
```

Récupérer quelques relevés réels (sans compte OpenSky, environ 100 relevés par jour sont autorisés) :

```bash
python -m flightstream.producer --sink file --out data/landing --interval 30 --max-polls 10
```

Traiter les fichiers avec le même code que sur Databricks :

```bash
python jobs/run_local.py --input data/landing --output data/gold
```

Exemple de sortie sur deux relevés pris un soir de novembre :

```
positions              4026 rows
quarantine             22 rows
airport_traffic_5min   44 rows
country_traffic_5min   103 rows

airport_icao  airport_name        peak_aircraft_5min
EGLL          London Heathrow     28
LSZH          Zurich              17
LEBL          Barcelona El Prat   16
EHAM          Amsterdam Schiphol  15
LEMD          Madrid Barajas      14
```

Le mode `--stream` exécute le chemin streaming (watermark, fenêtres, sortie console) sur le dossier
`data/landing` pendant que le producteur tourne. Sous Windows il demande les binaires Hadoop
(winutils), le plus simple est de le lancer depuis WSL.

## Déploiement sur Azure

1. **Infrastructure de base** :

   ```bash
   az login
   cd infra/terraform
   terraform init
   terraform apply
   ```

   Crée le resource group, Event Hubs (namespace Standard, hub `flights`, 4 partitions, consumer group
   `databricks`), ADLS Gen2, Key Vault (avec la chaîne de connexion en lecture), le workspace Databricks
   et un Azure Container Registry.

2. **Producteur** : construire l'image puis activer le conteneur.

   ```bash
   az acr build -r $(terraform output -raw acr_name) -t flight-producer:latest -f producer/Dockerfile ../..
   terraform apply -var deploy_producer=true
   ```

3. **Databricks** : créer un secret scope `flights` adossé au Key Vault (URI et ID en sortie Terraform),
   une external location Unity Catalog vers le conteneur `lakehouse`, le catalogue `flights`, puis :

   ```bash
   databricks bundle deploy -t dev
   ```

   Le job tourne en mode continu.

Le workflow `deploy.yml` enchaîne ces étapes avec une authentification OIDC
(secrets `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`).

Coût : Event Hubs Standard et un petit cluster Databricks qui tourne en continu ne sont pas gratuits.
Pour une démonstration, lancer le job avec `--available-now` à la demande plutôt qu'en continu.

## Power BI

Connexion DirectQuery au SQL warehouse Databricks sur les deux tables gold, rafraîchissement
automatique de la page toutes les minutes. Mesures dans `powerbi/measures.dax` : avions par aéroport
sur la dernière fenêtre, arrivées et départs, pic sur 5 minutes, part des arrivées, fraîcheur
des données, avions en vol par pays.

## Structure

```
src/flightstream/   client OpenSky, producteur, sinks (fichier, Event Hubs), transformations Spark
jobs/               job streaming Databricks et exécution locale
tests/              tests unitaires (qualité, dédoublonnage, aéroports, fenêtres, producteur)
data/airports.csv   41 aéroports (Europe, Maroc, Maghreb, Moyen-Orient, États-Unis, Asie)
producer/           Dockerfile du producteur
infra/terraform/    infrastructure Azure
powerbi/            mesures DAX
databricks.yml      définition du job (Databricks Asset Bundle)
```
