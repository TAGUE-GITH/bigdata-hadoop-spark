# Exercice 4 — Jointures, skew et partitionnement

_Résultats générés par l'exécution réelle du script (PySpark 4.2.0, local, N = 5 000 000 commandes). Les durées dépendent de la machine._

## Q1. Jointure subie (broadcast et AQE désactivés)

**Prédiction** : SortMergeJoin, avec 3 Exchange (un par côté de la jointure, plus un pour le `groupBy`). Dans le stage de jointure, une tâche reçoit toute la clé P000, soit environ 60 % des lignes, et dure bien plus longtemps que les autres.

```text
== Physical Plan ==
*(6) HashAggregate(keys=[famille#2450], functions=[sum((cast(quantite#2446 as double) * prix_unitaire#2447))])
+- Exchange hashpartitioning(famille#2450, 8), ENSURE_REQUIREMENTS, [plan_id=3904]
   +- *(5) HashAggregate(keys=[famille#2450], functions=[partial_sum((cast(quantite#2446 as double) * prix_unitaire#2447))])
      +- *(5) Project [quantite#2446, prix_unitaire#2447, famille#2450]
         +- *(5) SortMergeJoin [id_produit#2444], [id_produit#2449], Inner
            :- *(2) Sort [id_produit#2444 ASC NULLS FIRST], false, 0
            :  +- Exchange hashpartitioning(id_produit#2444, 8), ENSURE_REQUIREMENTS, [plan_id=3887]
            :     +- *(1) Filter isnotnull(id_produit#2444)
            :        +- InMemoryTableScan [id_produit#2444, quantite#2446, prix_unitaire#2447], [isnotnull(id_produit#2444)]
            :              +- InMemoryRelation [id_commande#2443, id_produit#2444, pays#2445, quantite#2446, prix_unitaire#2447, date_commande#2448], StorageLevel(disk, memory, deserialized, 1 replicas)
            :                    +- *(1) ColumnarToRow
            :                       +- FileScan parquet [id_commande#2443,id_produit#2444,pays#2445,quantite#2446,prix_unitaire#2447,date_commande#2448] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/./spark/dat..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id_commande:string,id_produit:string,pays:string,quantite:int,prix_unitaire:double,date_co...
            +- *(4) Sort [id_produit#2449 ASC NULLS FIRST], false, 0
               +- Exchange hashpartitioning(id_produit#2449, 8), ENSURE_REQUIREMENTS, [plan_id=3895]
                  +- *(3) Filter isnotnull(id_produit#2449)
                     +- InMemoryTableScan [id_produit#2449, famille#2450], [isnotnull(id_produit#2449)]
                           +- InMemoryRelation [id_produit#2449, famille#2450], StorageLevel(disk, memory, deserialized, 1 replicas)
                                 +- *(1) ColumnarToRow
                                    +- FileScan parquet [id_produit#2449,famille#2450] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/./spark/dat..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id_produit:string,famille:string>
```

| famille | CA |
|---|---|
| Cablage | 266 611 989.30 |
| Domotique | 266 852 468.68 |
| Eclairage | 1 864 710 277.79 |
| Protection | 266 082 438.46 |

Le CA d'Éclairage écrase les autres familles : P000 (id 0, donc 0 % 4 = Éclairage) porte 60 % des commandes. Le déséquilibre métier se retrouve directement dans le déséquilibre technique.

Plan : **SortMergeJoin**, **3 Exchange**. Dans le stage de jointure, la tâche la plus longue dure 5218 ms contre 580 ms pour la médiane. C'est la partition qui reçoit le hachage de `P000` : toutes les lignes d'une même clé vont dans la même partition.

## Q2. Jointure broadcast

```text
== Physical Plan ==
*(3) HashAggregate(keys=[famille#2450], functions=[sum((cast(quantite#2446 as double) * prix_unitaire#2447))])
+- Exchange hashpartitioning(famille#2450, 8), ENSURE_REQUIREMENTS, [plan_id=3985]
   +- *(2) HashAggregate(keys=[famille#2450], functions=[partial_sum((cast(quantite#2446 as double) * prix_unitaire#2447))])
      +- *(2) Project [quantite#2446, prix_unitaire#2447, famille#2450]
         +- *(2) BroadcastHashJoin [id_produit#2444], [id_produit#2449], Inner, BuildRight, false, false
            :- *(2) Filter isnotnull(id_produit#2444)
            :  +- InMemoryTableScan [id_produit#2444, quantite#2446, prix_unitaire#2447], [isnotnull(id_produit#2444)]
            :        +- InMemoryRelation [id_commande#2443, id_produit#2444, pays#2445, quantite#2446, prix_unitaire#2447, date_commande#2448], StorageLevel(disk, memory, deserialized, 1 replicas)
            :              +- *(1) ColumnarToRow
            :                 +- FileScan parquet [id_commande#2443,id_produit#2444,pays#2445,quantite#2446,prix_unitaire#2447,date_commande#2448] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/./spark/dat..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id_commande:string,id_produit:string,pays:string,quantite:int,prix_unitaire:double,date_co...
            +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=3979]
               +- *(1) Filter isnotnull(id_produit#2449)
                  +- InMemoryTableScan [id_produit#2449, famille#2450], [isnotnull(id_produit#2449)]
                        +- InMemoryRelation [id_produit#2449, famille#2450], StorageLevel(disk, memory, deserialized, 1 replicas)
                              +- *(1) ColumnarToRow
                                 +- FileScan parquet [id_produit#2449,famille#2450] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/./spark/dat..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id_produit:string,famille:string>
```

Plan : **BroadcastHashJoin**, 2 Exchange (`BroadcastExchange` de la petite table et l'échange du `groupBy`). Les 5 M commandes **ne sont plus redistribuées** pour la jointure : chaque tâche joint ses lignes avec une copie locale des 1 000 produits. Le skew disparaît, car aucune partition ne regroupe P000. Le seuil automatique `spark.sql.autoBroadcastJoinThreshold` vaut **10 Mo** par défaut. Si la « petite » table faisait en réalité 5 Go, le broadcast automatique ne s'appliquerait pas. Un broadcast forcé devrait rassembler 5 Go sur le Driver puis les copier dans chaque executor, avec un risque d'OOM et de saturation réseau.

## Q3. Diagnostiquer le skew

| id_produit | commandes | part |
|---|---|---|
| P000 | 2 999 796 | 60.0 % |
| P254 | 2 154 | 0.0 % |
| P487 | 2 136 | 0.0 % |
| P623 | 2 134 | 0.0 % |
| P888 | 2 127 | 0.0 % |

P000 concentre **60.0 %** des commandes, contre environ 0,04 % pour les autres produits. Sur la jointure subie, le ratio entre la tâche maximale et la tâche médiane vaut **9.0**. Un ratio supérieur à 3 à 5 est le signal d'un skew : le stage dure aussi longtemps que sa tâche la plus lente, pendant que les autres cœurs attendent.

## Q4. Corriger le skew (broadcast désactivé)

**(a) AQE + `skewJoin`**. Plan final AQE : `SortMergeJoin(skew=true)`. Spark a détecté la partition P000 comme déséquilibrée et l'a découpée en plusieurs tâches. La petite table est dupliquée en face de chaque morceau. Les seuils `skewedPartitionThresholdInBytes` (8 Mo) et `advisoryPartitionSizeInBytes` (4 Mo) ont été abaissés pour ce volume local. Les valeurs par défaut (256 Mo / 64 Mo) visent des données de production.

**(b) Salage**. On ajoute à `commandes` une colonne `salt` aléatoire de 0 à 7 et on duplique `produits` 8 fois. La jointure se fait sur `(id_produit, salt)` : P000 est réparti en 8 clés, donc sur 8 partitions. La dimension pèse 8 fois plus lourd, et le code est moins lisible.

**(c) Clé chaude traitée à part**. Les commandes P000 sont jointes en broadcast avec une dimension d'une ligne, et le reste par une jointure classique, désormais équilibrée. On termine par `unionByName`.

Contrôle : les trois corrections donnent **exactement le même CA par famille** que la jointure subie.

### Livrable — comparatif des stratégies

| Stratégie | Jointure | Exchange | Durée | Shuffle write | Tâche max | Tâche médiane | Max / médiane |
|---|---|---|---|---|---|---|---|
| Jointure subie (SortMergeJoin) | SortMergeJoin | 3 | 6.33 s | 57.8 Mo | 5 218 ms | 580 ms | 9.0 |
| Broadcast forcé | BroadcastHashJoin | 2 | 0.84 s | 1.1 Ko | 656 ms | 648 ms | 1.0 |
| (a) AQE skewJoin | SortMergeJoin | 6 | 2.22 s | 57.8 Mo | 716 ms | 333 ms | 2.2 |
| (b) Salage x8 | SortMergeJoin | 3 | 3.25 s | 63.2 Mo | 1 258 ms | 782 ms | 1.6 |
| (c) Clé chaude à part | BroadcastHashJoin | 4 | 2.24 s | 25.9 Mo | 597 ms | 472 ms | 1.3 |

Lecture : la colonne *Max / médiane* mesure le déséquilibre du stage le plus lent.

- **Broadcast** : la plus efficace quand la dimension est petite, puisqu'il n'y a plus de shuffle des faits.
- **AQE** : la plus simple, il suffit d'une configuration, mais la correction dépend des seuils.
- **Salage** : la plus générale, car elle marche aussi entre deux grandes tables, mais elle complique le code et multiplie la dimension.
- **Clé chaude à part** : efficace et explicite, mais il faut connaître les clés chaudes à l'avance.

## Q5. Nombre de partitions après shuffle

| Configuration | shuffle.partitions | Tâches de reduce | Durée |
|---|---|---|---|
| AQE false | 2 | 2 | 0.79 s |
| AQE false | 8 | 8 | 0.62 s |
| AQE false | 200 | 200 | 1.41 s |
| AQE true | 2 | 1 | 0.66 s |
| AQE true | 8 | 1 | 0.59 s |
| AQE true | 200 | 1 | 0.83 s |

Le résultat ne compte qu'environ 6 000 groupes (6 pays × 1 000 produits), soit quelques centaines de Ko. Avec 200 partitions sans AQE, Spark lance 200 tâches qui traitent chacune presque rien : le coût d'ordonnancement, de lancement et d'ouverture des blocs de shuffle domine. Avec l'AQE, `coalescePartitions` fusionne ces micro-partitions en quelques tâches, d'où un nombre de tâches de reduce indépendant du réglage.

## Q6. Contrôler le partitionnement

| Opération | Shuffle | Partitions non vides | Lignes par partition (min → max) |
|---|---|---|---|
| `repartition(8)` | oui | 8 | 625 000 → 625 000 |
| `repartition(8, 'pays')` | oui | 6 | 50 098 → 990 663 |
| `repartitionByRange(8, 'date_commande')` | oui | 8 | 574 329 → 666 200 |
| `coalesce(2)` | non | 2 | 2 500 000 → 2 500 000 |

`repartition(8)` équilibre parfaitement, au prix d'un shuffle complet. `repartition(8, 'pays')` hache la valeur du pays : il n'y a que **6 clés distinctes** (5 pays + NULL), donc au plus 6 partitions non vides. Deux pays peuvent même tomber dans la même partition par collision de hachage. `repartitionByRange` découpe par plages de dates de taille proche, en s'appuyant sur un échantillonnage. `coalesce(2)` fusionne des partitions existantes **sans shuffle** : c'est peu coûteux, mais rien ne garantit l'équilibre.

## Q7. Jointures semi, anti et non égale

Sur `commandes.limit(1000)` : **340** produits commandés (`left_semi`) et **660** jamais commandés (`left_anti`), soit 1000 = 1 000 produits.

`left_semi` et `left_anti` renvoient uniquement les colonnes de gauche, **sans dupliquer** une ligne qui a plusieurs correspondances. Spark peut arrêter la recherche dès la première correspondance. Un `left join` suivi d'un `filter(isNull)` matérialise toutes les correspondances avant d'en jeter la plupart.

Jointure non égale `a.x < b.y` : opérateur **CartesianProduct**. Sans clé d'égalité, on ne peut ni hacher ni trier par clé : chaque ligne de `a` est comparée à **toutes** les lignes de `b`, soit un coût en O(n × m). Avec deux tables d'un million de lignes, cela fait 10^12 comparaisons. On obtient ici un `CartesianProduct` parce que le broadcast automatique a été désactivé en Q1. Avec le broadcast actif, Spark choisit un `BroadcastNestedLoopJoin`, de même complexité, mais sans shuffle.

```text
== Physical Plan ==
CartesianProduct (x#7421L < y#7423L)
:- *(1) Project [id#7420L AS x#7421L]
:  +- *(1) Range (0, 5, step=1, splits=4)
+- *(2) Project [id#7422L AS y#7423L]
   +- *(2) Range (0, 5, step=1, splits=4)
```

