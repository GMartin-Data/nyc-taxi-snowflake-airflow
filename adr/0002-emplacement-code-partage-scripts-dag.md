# ADR-0002 : Emplacement du code partagé entre les scripts d'ingestion et le DAG

Status: Accepted
Date: 2026-10-07

## Contexte

Le DAG de chargement du jour 3 a besoin de deux morceaux de logique écrits au
jour 2 dans `ingestion/` : `file_name(month)`, la frontière qui valide le
format `AAAA-MM` avant toute interpolation dans le SQL, et `copy_into`, qui
porte la garde `_source_file` décidée par ADR-0001. L'ADR-0001 anticipait
d'ailleurs que « le DAG du jour 3 hérite de la garantie en réutilisant le
loader ».

Or le conteneur Airflow ne voit pas `ingestion/`. Astro CLI construit l'image
avec `airflow/` pour contexte de build, sans option pour l'élargir ; le
`Dockerfile` ne peut rien copier depuis le dossier parent. En revanche
`airflow/include/` est copié dans l'image au build, monté dans les conteneurs
en développement, et `/usr/local/airflow` est sur `sys.path` : tout module
`airflow/include/x.py` s'importe `from include.x import ...` depuis un DAG.
Le kit y range déjà son SQL (`include/sql/`).

Deux faits rendent la réutilisation directe possible : `SnowflakeHook.get_conn()`
renvoie la `SnowflakeConnection` du connecteur, le type que `put(conn, file)`
et `copy_into(conn, ...)` prennent déjà ; et l'image embarque `requests`,
`structlog` et le connecteur Snowflake. Seul `connect()` (clé lue dans un
fichier du poste) est propre aux scripts.

Enfin `ingestion/load_month.py` mélange trois rôles : source (`file_name`,
`source_url`, `download`), orchestration (`main`) et `DATA_DIR` ; `load_zones.py`
importe `download` depuis ce module de script (constat [6] de la revue du jour 2,
reporté à cette décision).

## Options considérées

- **Option A — duplication bornée dans `airflow/include/`** : recopier
  `file_name` et `copy_into` avec sa garde ; `ingestion/` intact. Coût nul
  aujourd'hui ; mais la garantie ADR-0001 vivrait dans deux codes, avec une
  dérive silencieuse possible, des tests à dupliquer côté image, et le
  constat [6] resterait ouvert.
- **Option B — bibliothèque dans `airflow/include/`, scripts dans `ingestion/`** :
  `tlc.py` et `snowflake_loader.py` déplacés dans `include/`, les scripts
  restent et les importent. Une seule copie ; mais chaque commande hôte doit
  porter `PYTHONPATH=airflow/include`, et le code Python a deux racines.
- **Option C — tout `ingestion/` déplacé dans `airflow/include/`** : les
  modules sont découpés en source (`tlc.py`), puits (`snowflake_loader.py`)
  et orchestration (`load_month.py`, `load_zones.py`, `check_connection.py`),
  tous dans `include/`. Une seule racine ; le DAG importe `include.tlc` et
  `include.snowflake_loader` ; les scripts se lancent depuis `airflow/include/`
  (leur dossier est sur `sys.path`, les imports frères fonctionnent). Coût :
  un déplacement de fichiers et des chemins à mettre à jour.

## Décision

Option C. La logique qui porte une garantie n'existe qu'en un seul exemplaire,
dans le dossier que le kit destine à ce que les DAG partagent ; aucune variable
d'environnement à expliquer ; la découpe source / puits / orchestration rend
chaque module importable isolément. `connect()` reste réservé aux scripts, le
DAG passe la connexion du hook. L'option A est écartée parce qu'elle duplique
précisément le code qui porte une décision d'architecture ; l'option B n'apporte
qu'un dossier `ingestion/` visible, au prix d'une seconde racine de code.

## Conséquences

- `git mv ingestion/* airflow/include/`, puis découpe de `tlc.py` hors de
  `load_month.py`. Aucun contrat ne change : les 13 tests du poste et les
  3 tests de DAG restent verts, c'est le critère de l'opération.
- `pytest.ini` passe à `pythonpath = airflow/include` ; `ruff.toml` déclare les
  nouveaux modules et `include` en first-party ; `DATA_DIR` est recalculé
  depuis la racine du dépôt.
- Commandes hôte : `uv run --env-file .env --with-requirements
  airflow/include/requirements.txt python airflow/include/load_month.py 2025-01`
  (idem `load_zones.py`) ; le README du jour 5 les documente.
- Le DAG n'importe jamais les modules d'orchestration (leurs imports frères ne
  se résolvent pas depuis un DAG), seulement `tlc` et `snowflake_loader`.
- Les journaux structlog du loader apparaîtront dans le log de tâche Airflow
  (Airflow 3 journalise avec structlog) : à observer au premier run.
- Trade-off : `ingestion/` disparaît comme dossier-livrable du jour 2 ; sa
  trace reste dans l'historique Git et le README.
