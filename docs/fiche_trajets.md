# Fiche source — Trajets des taxis jaunes de New York (TLC Yellow Taxi Trip Records)

Périmètre du projet : janvier, février et mars 2025. Tous les chiffres de cette fiche ont été mesurés le 5 octobre 2026, avec les commandes indiquées ; aucun n'est estimé. L'exploration détaillée (colonnes, codes, surprises) porte sur le fichier de janvier 2025.

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | Yellow Taxi Trip Records : un enregistrement par trajet de taxi jaune |
| Producteur des données | NYC Taxi and Limousine Commission (TLC). Les enregistrements sont collectés par les prestataires technologiques agréés (programme TPEP) puis transmis à la TLC, qui précise ne pas garantir leur exactitude. |
| Adresse (URL) | Page : <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page><br>Fichier d'un mois : `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_AAAA-MM.parquet` |
| Accès (public, authentifié) | Public, en HTTPS, sans authentification |
| Format du fichier | Parquet (format version 2, écrit par `parquet-cpp-arrow 16.1.0`), compression ZSTD, 4 groupes de lignes pour janvier |
| Fréquence de publication | Mensuelle : un fichier par mois de prise en charge |
| Délai entre la période couverte et la publication | Environ deux mois d'après la TLC (« typically with a two-month delay »). Dernière modification constatée (en-tête HTTP `Last-Modified`) : 23 avril 2025 pour janvier et février, 21 mai 2025 pour mars. |

## Volume mesuré

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---|---|---|---|
| `yellow_tripdata_2025-01.parquet` | 59 158 238 octets (59,2 Mo) | 3 475 226 | 20 | `ls -l` et DuckDB, sur le fichier téléchargé |
| `yellow_tripdata_2025-02.parquet` | 60 343 086 octets (60,3 Mo) | 3 577 543 | 20 | `curl -sI` et DuckDB, sur le fichier distant |
| `yellow_tripdata_2025-03.parquet` | 69 964 745 octets (70,0 Mo) | 4 145 257 | 20 | `curl -sI` et DuckDB, sur le fichier distant |
| **Total des trois mois** | 189 466 069 octets (189,5 Mo) | **11 198 026** | | |

Commandes (DuckDB lit le pied du fichier Parquet : compter les lignes ne charge pas les données en mémoire) :

```bash
URL=https://d37ci6vzurychx.cloudfront.net/trip-data

# Taille : fichier local, puis fichier distant sans le télécharger
ls -l data/yellow_tripdata_2025-01.parquet
curl -sI $URL/yellow_tripdata_2025-02.parquet | grep -i content-length

# Nombre de lignes, puis colonnes et types tels qu'ils sont écrits dans le fichier
uv run --with duckdb python -c "import duckdb; print(duckdb.sql(\"SELECT COUNT(*) FROM 'data/yellow_tripdata_2025-01.parquet'\"))"
uv run --with duckdb python -c "import duckdb; duckdb.sql(\"SELECT name, type, logical_type FROM parquet_schema('data/yellow_tripdata_2025-01.parquet')\").show(max_rows=30, max_width=200)"

# Même comptage sur un fichier distant
uv run --with duckdb python -c "import duckdb; print(duckdb.sql(\"SELECT COUNT(*) FROM '$URL/yellow_tripdata_2025-02.parquet'\"))"
```

Sans `uv` : `pip install duckdb`, puis les mêmes commandes avec `python3 -c`.

## Colonnes

Les noms sont ceux du fichier, avec leur casse d'origine (`VendorID`, `PULocationID`, `Airport_fee`) : elle n'est pas homogène, et le dictionnaire de données écrit `airport_fee`. Les 20 colonnes ont le même nom et le même type dans les fichiers de janvier, février et mars 2025.

| Colonne | Type dans le fichier | Signification | Exemple de valeur |
|---|---|---|---|
| `VendorID` | `INT32` | Code du prestataire TPEP qui a transmis l'enregistrement | `1` |
| `tpep_pickup_datetime` | `INT64`, date et heure à la microseconde, sans fuseau | Date et heure d'enclenchement du compteur | `2025-01-01 00:18:38` |
| `tpep_dropoff_datetime` | `INT64`, date et heure à la microseconde, sans fuseau | Date et heure d'arrêt du compteur | `2025-01-01 00:26:59` |
| `passenger_count` | `INT64` | Nombre de passagers dans le véhicule (déclaré par le chauffeur) | `1` |
| `trip_distance` | `DOUBLE` | Distance parcourue, en miles, relevée par le taximètre | `1.6` |
| `RatecodeID` | `INT64` | Code du tarif en vigueur à la fin du trajet | `1` |
| `store_and_fwd_flag` | `BYTE_ARRAY`, texte | Enregistrement gardé en mémoire dans le véhicule avant envoi, faute de connexion au serveur | `N` |
| `PULocationID` | `INT32` | Zone TLC où le compteur a été enclenché (prise en charge) | `229` |
| `DOLocationID` | `INT32` | Zone TLC où le compteur a été arrêté (dépose) | `237` |
| `payment_type` | `INT64` | Code du mode de paiement | `1` |
| `fare_amount` | `DOUBLE` | Prix de la course au temps et à la distance, calculé par le compteur | `10.0` |
| `extra` | `DOUBLE` | Suppléments et majorations divers | `3.5` |
| `mta_tax` | `DOUBLE` | Taxe déclenchée automatiquement selon le tarif au compteur | `0.5` |
| `tip_amount` | `DOUBLE` | Pourboire, renseigné automatiquement pour les paiements par carte ; les pourboires en espèces n'y figurent pas | `3.0` |
| `tolls_amount` | `DOUBLE` | Total des péages payés pendant le trajet | `0.0` |
| `improvement_surcharge` | `DOUBLE` | Surtaxe d'amélioration appliquée à la prise en charge (depuis 2015) | `1.0` |
| `total_amount` | `DOUBLE` | Montant total facturé aux passagers, hors pourboires en espèces | `18.0` |
| `congestion_surcharge` | `DOUBLE` | Montant perçu au titre de la surtaxe de congestion de l'État de New York | `2.5` |
| `Airport_fee` | `DOUBLE` | Frais appliqués aux prises en charge aux aéroports LaGuardia et JFK | `0.0` |
| `cbd_congestion_fee` | `DOUBLE` | Frais par trajet pour la zone de décongestion de la MTA, depuis le 5 janvier 2025 ; colonne ajoutée à partir des données 2025 | `0.0` |

## Codes

Signification d'après le dictionnaire de données de la TLC (version du 18 mars 2025) : <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf>. La dernière colonne compte les lignes du fichier de janvier 2025.

| Colonne | Valeur | Signification | Lignes en janvier 2025 |
|---|---|---|---|
| `VendorID` | 1 | Creative Mobile Technologies, LLC | 753 671 |
| `VendorID` | 2 | Curb Mobility, LLC | 2 719 860 |
| `VendorID` | 6 | Myle Technologies Inc | 489 |
| `VendorID` | 7 | Helix | 1 206 |
| `RatecodeID` | 1 | Tarif standard | 2 756 472 |
| `RatecodeID` | 2 | JFK | 94 420 |
| `RatecodeID` | 3 | Newark | 8 622 |
| `RatecodeID` | 4 | Nassau ou Westchester | 7 092 |
| `RatecodeID` | 5 | Tarif négocié | 26 501 |
| `RatecodeID` | 6 | Course groupée (« Group ride ») | 7 |
| `RatecodeID` | 99 | Nul ou inconnu | 41 963 |
| `RatecodeID` | vide | Absent du dictionnaire | 540 149 |
| `store_and_fwd_flag` | Y | Enregistrement gardé en mémoire puis envoyé | 7 646 |
| `store_and_fwd_flag` | N | Enregistrement envoyé directement | 2 927 431 |
| `store_and_fwd_flag` | vide | Absent du dictionnaire | 540 149 |
| `payment_type` | 0 | « Flex Fare trip » | 540 149 |
| `payment_type` | 1 | Carte bancaire | 2 444 393 |
| `payment_type` | 2 | Espèces | 390 429 |
| `payment_type` | 3 | Sans frais (« No charge ») | 23 773 |
| `payment_type` | 4 | Litige (« Dispute ») | 76 481 |
| `payment_type` | 5 | Inconnu | 1 |
| `payment_type` | 6 | Trajet annulé (« Voided trip ») | 0 |

## Ce qui a surpris

Constats sur le fichier de janvier 2025 (3 475 226 lignes) :

- **Dates hors période** : 22 trajets ont une prise en charge hors de janvier 2025 (21 en décembre 2024, 1 en février 2025). Le mois d'un trajet ne se déduit donc pas sûrement de sa date.
- **Un bloc de lignes incomplètes** : les 540 149 lignes de `payment_type = 0` (15,54 %) sont exactement celles où `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` et `Airport_fee` sont vides.
- **Montants négatifs ou aberrants** : `fare_amount` est négatif sur 144 118 lignes et `total_amount` sur 63 037 ; `total_amount` va de -901,00 à 863 380,37.
- **Distances impossibles** : 90 893 trajets de distance nulle, 162 de plus de 100 miles, et un maximum de 276 423,57 miles.
- **Durées impossibles** : 2 051 trajets dont l'arrivée précède ou égale le départ, 1 375 de plus de trois heures.
- **Passagers** : 24 656 trajets déclarés avec 0 passager.
- **Pas de doublon exact** : aucune ligne n'est strictement identique à une autre.
