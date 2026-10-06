# ADR-0001 : Garantie de rejeu du chargement RAW

Status: Proposed
Date: 2026-10-06

## Contexte

Le chargement de la couche RAW (`PUT` puis `COPY INTO`, dans
`ingestion/snowflake_loader.py`) est annoncé rejouable : recharger un mois
déjà présent n'ajoute aucune ligne. `CONTRAT_RAW.md` impose une copie fidèle
des fichiers : un fichier chargé, ses lignes une seule fois.

Cette propriété repose aujourd'hui sur un seul mécanisme : l'historique de
chargement que Snowflake tient par table (les fichiers déjà copiés sont
sautés par `COPY INTO`). Snowflake ne conserve cet historique que 64 jours.
Passé ce délai, un rejeu (reprise d'historique Airflow, relance manuelle)
double les lignes du mois. Le SQL aval dédoublonne sur `_loaded_at` en
gardant la première version, ce qui masquerait le symptôme sans corriger RAW.

Le brief prévoit des chargements mois par mois orchestrés par Airflow (jour 3) :
des rejeux tardifs sont un état atteignable, pas un cas théorique.

## Options considérées

- **Option A — assumer la borne et la documenter** : aucun code ; docstrings
  et note 04 précisent « rejouable sous 64 jours ». Simple ; mais la promesse
  devient conditionnelle, et sa rupture est silencieuse.
- **Option B — garde explicite dans le loader** : avant `COPY INTO`, compter
  les lignes de la table dont `_source_file` vaut le nom du fichier ; si
  supérieur à zéro, ne rien copier. La propriété est portée par le code,
  indépendamment de l'historique Snowflake ; une requête de plus par
  chargement ; deux chargements simultanés du même fichier ne sont pas
  couverts (la garde et la copie ne sont pas atomiques).
- **Option C — `DELETE` du mois puis `COPY INTO ... FORCE = TRUE`** : le
  pattern « DELETE puis INSERT » du SQL aval du brief. Idempotent sans
  dépendre de l'historique ; mais perd le premier `_loaded_at`, réécrit RAW
  à chaque rejeu et recharge intégralement un fichier déjà présent.

## Décision

Option B. RAW garantit « jamais deux fois les lignes d'un même fichier », et
c'est le loader qui porte cette garantie : il vérifie la présence de
`_source_file` avant de copier. L'historique de chargement Snowflake reste
actif en second rideau. L'option A laisse une promesse fausse après 64 jours ;
l'option C contredit la fidélité de RAW (perte du premier `_loaded_at`) pour
un bénéfice que B obtient sans réécriture.

## Conséquences

- `snowflake_loader.copy_into` (ou `load_month.main`) acquiert une garde sur
  `_source_file` ; les docstrings « a month already loaded adds no row »
  deviennent vraies sans condition.
- Un test unitaire à faux curseur couvre les deux branches (fichier présent,
  fichier absent), ce qui répond aussi au constat [5] de la revue.
- Une requête `COUNT` de plus par chargement : coût négligeable sur XS.
- La concurrence entre deux chargements du même fichier n'est pas couverte :
  l'orchestrateur sérialise les mois, trade-off accepté.
- Le DAG du jour 3 hérite de la garantie en réutilisant le loader ; l'étape D
  de `04_verify_raw_contract.sql` reste valide (un rejeu sauté ne laisse
  aucune ligne dans `COPY_HISTORY`).
