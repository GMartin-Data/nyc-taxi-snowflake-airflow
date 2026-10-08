# Réponse à la direction d'Hudson Cab Partners

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## La requête

Une ligne par zone de prise en charge et heure de la journée, sur les trois mois chargés. Le mode de paiement est mis en colonnes : recette moyenne d'un trajet payé par carte, d'un trajet payé en espèces, et part des trajets payés par carte.

```sql
SELECT
    z.zone_name                                     AS pickup_zone,
    z.borough,
    f.pickup_hour,
    COUNT(*)                                        AS nb_trips,
    ROUND(COUNT(*) / COUNT(DISTINCT f.pickup_date)) AS trips_per_day,
    ROUND(AVG(f.total_amount), 2)                   AS avg_revenue_per_trip,
    ROUND(AVG(CASE WHEN p.payment_type_label = 'Credit card' THEN f.total_amount END), 2) AS avg_revenue_card,
    ROUND(AVG(CASE WHEN p.payment_type_label = 'Cash' THEN f.total_amount END), 2)        AS avg_revenue_cash,
    ROUND(100 * COUNT_IF(p.payment_type_label = 'Credit card') / COUNT(*), 1)             AS pct_card
FROM NYC_TAXI.MARTS.FCT_TRIPS f
LEFT JOIN NYC_TAXI.MARTS.DIM_ZONE z         ON z.zone_key = f.pickup_zone_key
LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p ON p.payment_type_key = f.payment_type_key
GROUP BY 1, 2, 3
ORDER BY nb_trips DESC
LIMIT 10;
```

Contrôle : sans `LIMIT`, la somme de `nb_trips` sur les 5 911 lignes vaut 10 382 378, le nombre de trajets de `FCT_TRIPS`. Le top 10 coïncide avec `MART_ZONE_HOURLY_DEMAND` agrégée sur `is_weekend`.

## Le résultat : les 10 premières lignes

Exécutée le 2026-10-08 sur les fichiers de janvier à mars 2025. Montants en dollars.

| Zone | Arrondissement | Heure | Trajets | Trajets par jour | Recette moyenne | Par carte | En espèces | Part carte |
|---|---|---|---|---|---|---|---|---|
| Midtown Center | Manhattan | 18 h | 45 978 | 511 | 24,40 | 25,06 | 19,93 | 83,2 % |
| Midtown Center | Manhattan | 17 h | 45 063 | 501 | 28,57 | 26,35 | 20,74 | 84,4 % |
| Midtown Center | Manhattan | 19 h | 38 770 | 431 | 23,62 | 24,25 | 19,36 | 81,7 % |
| Midtown Center | Manhattan | 20 h | 38 518 | 428 | 22,67 | 23,61 | 18,61 | 72,3 % |
| Midtown Center | Manhattan | 16 h | 36 702 | 408 | 25,77 | 26,53 | 21,27 | 83,6 % |
| Upper East Side North | Manhattan | 15 h | 36 598 | 407 | 20,37 | 20,64 | 17,12 | 81,3 % |
| Upper East Side South | Manhattan | 14 h | 36 482 | 405 | 19,98 | 20,41 | 16,85 | 81,8 % |
| Upper East Side South | Manhattan | 15 h | 36 399 | 404 | 19,98 | 20,34 | 16,94 | 82,4 % |
| Times Sq/Theatre District | Manhattan | 21 h | 36 340 | 404 | 23,58 | 24,50 | 18,94 | 71,2 % |
| Upper East Side South | Manhattan | 18 h | 36 093 | 401 | 21,23 | 21,54 | 17,97 | 81,9 % |

## Ce qu'il faut en retenir

Trois phrases, pour quelqu'un qui ne lit pas le SQL :

1. La demande se concentre à Manhattan (88 % des prises en charge) et en fin d'après-midi : les cinq créneaux les plus chargés sont tous Midtown Center entre 16 h et 20 h, avec un pic à 18 h de 511 trajets par jour ; sur l'ensemble de la ville, la tranche 16 h-21 h représente 38 % des trajets de la journée.
2. Un trajet rapporte en moyenne 27 $, et de 20 à 29 $ dans ces créneaux de pointe : les courses de l'Upper East Side sont courtes (environ 20 $), celles de Midtown à 17 h sont les mieux payées du top 10 (28,57 $) ; les trajets les plus rémunérateurs sont ceux de 4 h-5 h du matin (31 à 35 $), mais ils sont dix fois moins nombreux.
3. La carte est le mode de paiement dominant (71 % des trajets, 81 à 84 % dans les créneaux de pointe, un peu moins en soirée à Times Square et Midtown à 20 h) et un trajet payé par carte rapporte 4 à 6 $ de plus qu'en espèces, en bonne partie parce que le pourboire par carte est enregistré alors que le pourboire en espèces ne l'est pas.

## Les limites

- **Période** : du 1er janvier au 31 mars 2025 (90 jours), soit l'hiver ; les saisons touristiques et estivales ne sont pas couvertes.
- **Trajets écartés** : 815 648 trajets (7,3 % des lignes des trois fichiers) sont exclus par les contrôles de qualité : montant nul ou négatif, distance hors de 0 à 100 miles, durée nulle ou supérieure à 3 heures, prise en charge hors du mois du fichier. Ils sont comptés dans `MART_DATA_QUALITY`.
- **Zones inconnues** : 23 318 trajets (0,2 %) partent des zones 264 et 265, « Unknown » et « N/A » ; ils sont dans les totaux mais ne peuvent être placés sur la carte.
- **Pourboires en espèces** : non saisis par les compteurs, donc la recette des trajets en espèces est sous-estimée.
- **Flex Fare** : 17 % des trajets ont le code de paiement 0, « Flex Fare trip », ni carte ni espèces ; la part carte et la part espèces ne se complètent donc pas à 100 %.
- **Périmètre** : les fichiers de la TLC couvrent tous les taxis jaunes de New York, pas seulement les 180 véhicules d'Hudson Cab Partners ; la réponse décrit le marché, pas la flotte.
