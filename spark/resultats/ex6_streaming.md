# Exercice 6 — Structured Streaming

_Résultats générés par l'exécution réelle du script (PySpark 4.2.0, local, N = 5 000 000 commandes). Les durées dépendent de la machine._

_Générateur de l'énoncé lancé dans un thread : 15 fichiers de 20 événements, un toutes les 2 s (au lieu de 40 toutes les 5 s, pour limiter la durée d'exécution). Les retards de 0 s, 30 s, 90 s et 10 min sont inchangés._

## Q1. Premier flux (sink mémoire, append, trigger 2 s)

```json
{
  "batchId": 14,
  "numInputRows": 20,
  "inputRowsPerSecond": 10.0,
  "processedRowsPerSecond": 73.0
}
```

Total reçu : **300 événements** sur 300 générés. Le **schéma est obligatoire** pour une source de fichiers : Spark ne peut pas inférer le schéma d'un dossier qui grossit sans relire les données à chaque batch, et un flux doit garder un plan stable du début à la fin. `maxFilesPerTrigger = 1` limite chaque micro-batch à un seul fichier. Cela régule le débit et rend chaque batch observable, au lieu de tout absorber d'un coup au démarrage. Sur Colab ou sur un sink de test, le sink **memory** remplace la console.

## Q2. Fenêtres d'une minute et modes de sortie

| Mode | Lignes dans la table résultat | Comportement |
|---|---|---|
| complete | 15 | toute la table d'agrégats est réécrite à chaque batch |
| update | 126 | seules les fenêtres modifiées sont émises ; une fenêtre peut apparaître plusieurs fois |
| append (sans watermark) | — | **refusé** : `[STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION] Invalid streaming output mode: append. This output mode is not supported for streaming aggregations without watermark on streaming DataFrames/DataSets. SQLSTATE: 42KDE` |

Le mode **append** n'émet une ligne qu'une fois, quand elle est **définitive**. Sans watermark, une fenêtre n'est jamais définitive, car un événement en retard peut toujours la modifier. Spark refuse donc la requête dès `start()`. **complete** renvoie tout à chaque batch, ce qui est coûteux et suppose un état complet. **update** renvoie seulement ce qui a changé.

Extrait de la table `complete` :

| window | machine | temp_moy | n |
|---|---|---|---|
| 09:38 → 09:39 | M1 | 76.6 | 5 |
| 09:38 → 09:39 | M2 | 66.8 | 6 |
| 09:38 → 09:39 | M3 | 86.1 | 1 |
| 09:39 → 09:40 | M1 | 78.3 | 9 |
| 09:39 → 09:40 | M2 | 76.0 | 12 |
| 09:39 → 09:40 | M3 | 76.3 | 11 |
| 09:47 → 09:48 | M1 | 73.3 | 19 |
| 09:47 → 09:48 | M2 | 79.8 | 13 |
| 09:47 → 09:48 | M3 | 73.3 | 18 |

## Q3. Watermark de 2 minutes

| Retard généré | Événements |
|---|---|
| 0 s | 161 |
| 30 s | 45 |
| 90 s | 50 |
| 600 s | 44 |

| Indicateur | Sans watermark | Avec watermark 2 min |
|---|---|---|
| Événements comptés dans les fenêtres | 300 | 257 |
| Événements ignorés | 0 | 43 |
| Métrique `numRowsDroppedByWatermark` (lignes pré-agrégées) | 0 | 30 |
| État : `numRowsTotal` (premiers → derniers batchs) | 7 → 8 → 15 → 15 | 7 → 6 → 9 → 9 |
| Fenêtres émises en append (définitives) | refusé | 1 |

Le watermark vaut *max(event_time vu) − 2 min*. Les retards de 0, 30 et 90 s restent sous cette limite : ils sont **comptés**. Les retards de 10 min tombent avant le watermark : ils sont **ignorés** : 43 sur les 44 générés avec 10 min de retard. Ceux du tout premier batch passent encore, car le watermark n'avance qu'à la fin d'un batch. La métrique `numRowsDroppedByWatermark` affiche 30 et non le nombre d'événements : elle compte les lignes après l'**agrégation partielle** du micro-batch (une par fenêtre et par machine), pas les événements bruts. L'état se **stabilise**, car Spark supprime les fenêtres plus anciennes que le watermark. Sans watermark, chaque nouvelle fenêtre reste en mémoire pour toujours : sur un flux de plusieurs jours, l'état grossirait sans limite jusqu'à saturer la mémoire.

## Q4. Checkpoint et reprise

| Scénario | Résultat |
|---|---|
| Arrêt après le batch 4 | 180 lignes écrites |
| Relance avec **le même** checkpoint | 300 lignes, 300 `event_id` distincts → **ni doublon ni trou** |
| Relance avec un checkpoint **neuf** (autre sortie) | 300 lignes : tout est retraité depuis le début |
| Fenêtre 1 min → 5 min, même checkpoint | `This query does not support recovering from checkpoint location. Delete ./spark/data/iot/ck_agg/offsets to start over.` |

Le checkpoint mémorise les **offsets**, c'est-à-dire les fichiers déjà lus, les batchs commis et l'**état** des agrégations. Après un arrêt, la requête reprend exactement au batch suivant. Le sink fichier tient en plus son propre journal (`_spark_metadata`), ce qui garantit l'exactly-once. Un checkpoint neuf ne sait rien du passé et relit tout. Changer la forme de l'agrégation rend l'**état stocké incompatible** avec la nouvelle requête : Spark refuse de repartir. Il faut alors un nouveau checkpoint.

## Q5. foreachBatch idempotent

2 dossiers `batch_id=…` écrits sur 15 micro-batchs, soit 2 alertes au total (machines dont la moyenne dépasse 80 °C dans le micro-batch). On rejoue ensuite le batch 9 : il avait 1 alerte(s) avant le rejeu et en a **1** après. La partition `batch_id=9` a été **écrasée**, pas complétée.

Après une panne, Spark peut **rejouer** le dernier micro-batch non commis dans le checkpoint. `foreachBatch` exécute du code libre, sans la garantie exactly-once du sink fichier. Une écriture en `append` produirait donc des doublons. Le `batch_id` est **stable** entre deux exécutions d'un même batch. L'utiliser comme partition écrasée en `overwrite`, ou comme clé de déduplication, rend l'écriture **idempotente**.

## Q6. Déduplication et jointure flux-statique

Un fichier déjà reçu (`b000.json`) est redéposé sous un autre nom. Après `dropDuplicates(['event_id'])` avec watermark, on obtient 261 lignes pour 261 `event_id` distincts. Les 20 doublons sont éliminés, et les événements en retard de plus de 2 min sont exclus par le watermark.

| machine_id | site | atelier | événements |
|---|---|---|---|
| M1 | Limoges | Atelier A | 84 |
| M2 | Limoges | Atelier B | 95 |
| M3 | Brive | Atelier C | 82 |

Plan physique de la requête : **BroadcastHashJoin** pour la jointure flux-statique et **StreamingDeduplicate** avec état. La table `machines` est petite et statique : elle est diffusée à chaque micro-batch, et **seul le flux garde un état**, celui des `event_id` vus, purgé par le watermark.

Une **jointure externe flux-flux** doit savoir quand une ligne n'aura *jamais* de correspondance pour émettre le NULL. Sans condition de temps entre les deux flux et sans watermark, cette réponse n'arrive jamais : Spark devrait garder **tout** l'historique des deux côtés, soit un état infini. Il refuse donc ces requêtes.

## Livrable — `lastProgress` commenté (requête avec watermark)

```json
{
  "batchId": 29,
  "numInputRows": 0,
  "eventTime": {
    "watermark": "2026-10-22T07:47:21.000Z"
  },
  "stateOperators": [
    {
      "operatorName": "stateStoreSave",
      "numRowsTotal": 9,
      "numRowsUpdated": 0,
      "numRowsRemoved": 0,
      "allUpdatesTimeMs": 38,
      "allRemovalsTimeMs": 2,
      "commitTimeMs": 72,
      "memoryUsedBytes": 6568,
      "numRowsDroppedByWatermark": 0,
      "numShufflePartitions": 8,
      "numStateStoreInstances": 8,
      "customMetrics": {
        "SnapshotLastUploaded.partition_2_default": -1,
        "SnapshotLastUploaded.partition_4_default": -1,
        "SnapshotLastUploaded.partition_5_default": -1,
        "SnapshotLastUploaded.partition_6_default": -1,
        "SnapshotLastUploaded.partition_7_default": -1,
        "forceSnapshotCount": 0,
        "loadedMapCacheHitCount": 464,
        "loadedMapCacheMissCount": 0,
        "numSnapshotsAutoRepaired": 0,
        "stateOnCurrentVersionSizeBytes": 3488
      }
    }
  ]
}
```

`eventTime.watermark` indique la limite courante, à 2 minutes sous le maximum observé. `stateOperators[0].numRowsTotal` donne la taille de l'état : elle reste bornée. `numRowsDroppedByWatermark` compte les événements trop tardifs.

