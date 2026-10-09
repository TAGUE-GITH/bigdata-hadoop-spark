# Exercice 2 — RDD, lignage et variables partagées

_Résultats générés par l'exécution réelle du script (PySpark 4.2.0, local, N = 5 000 000 commandes). Les durées dépendent de la machine._

## Q1. WordCount avec `reduceByKey`

```python
wc = lignes.flatMap(lambda l: l.split()).map(lambda m: (m, 1)).reduceByKey(lambda a, b: a + b)
wc.takeOrdered(5, key=lambda kv: -kv[1])
```

| Mot | Occurrences |
|---|---|
| chat | 926 871 |
| chien | 371 251 |
| mot63 | 18 916 |
| mot107 | 18 869 |
| mot154 | 18 826 |

Contrôle : 5 000 000 mots tirés ; « chat » a un poids de 50/270, soit environ 925 926 attendus.

## Q2. `reduceByKey` contre `groupByKey`

**Prédiction** : `reduceByKey` écrit beaucoup moins de données au shuffle. Il combine (mot, 1) localement dans chaque partition, donc au plus 272 paires par partition traversent le réseau. `groupByKey` envoie les 5 millions de paires.

| Version | Shuffle Write | Shuffle Read | Durée |
|---|---|---|---|
| reduceByKey | 20.3 Ko | 20.3 Ko | 19.56 s |
| groupByKey + mapValues(sum) | 79.2 Ko | 79.2 Ko | 20.21 s |

`groupByKey` écrit **4 fois plus** de données au shuffle. Le **combiner côté map** de `reduceByKey` additionne les 1 de chaque mot dans la partition avant l'échange : seuls des sous-totaux traversent le réseau.

**Écart avec la prédiction** : l'écart attendu était bien plus grand. Il est réduit pour deux raisons. D'abord, en PySpark, `groupByKey` regroupe déjà les valeurs en listes dans chaque partition avant l'échange. Ensuite, ces listes de 1 très répétitives sont sérialisées par lots et compressées (lz4), ce qui réduit fortement leur taille. Avec des valeurs variées (montants, identifiants) ou en Scala, la différence serait proportionnelle au nombre de paires. Les durées proches, environ 20 s, sont dominées par les échanges entre la JVM et les workers Python, qui sont lents sous Windows.

## Q3. Lignage (`toDebugString`)

**reduceByKey**
```text
(8) PythonRDD[24] at RDD at PythonRDD.scala:59 []
 |  MapPartitionsRDD[19] at mapPartitions at PythonRDD.scala:197 []
 |  ShuffledRDD[18] at partitionBy at DirectMethodHandleAccessor.java:103 []
 +-(8) PairwiseRDD[17] at reduceByKey at .\spark\ex2_rdd.py:72 []
    |  PythonRDD[16] at reduceByKey at .\spark\ex2_rdd.py:72 []
    |  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:326 []
```

**groupByKey**
```text
(8) PythonRDD[25] at RDD at PythonRDD.scala:59 []
 |  MapPartitionsRDD[23] at mapPartitions at PythonRDD.scala:197 []
 |  ShuffledRDD[22] at partitionBy at DirectMethodHandleAccessor.java:103 []
 +-(8) PairwiseRDD[21] at groupByKey at .\spark\ex2_rdd.py:73 []
    |  PythonRDD[20] at groupByKey at .\spark\ex2_rdd.py:73 []
    |  ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:326 []
```

Le changement d'indentation `+-(8)` marque le **ShuffledRDD**, c'est-à-dire la frontière de stage. Au-dessus, le stage reduce ; en dessous, le stage map (`ParallelCollectionRDD` → `PythonRDD` pour flatMap + map). C'est cette recette que Spark rejoue pour recalculer une partition perdue.

| Transformation | Type | Pourquoi |
|---|---|---|
| map | étroite | 1 partition d'entrée → 1 partition de sortie |
| filter | étroite | garde ou jette des lignes sur place |
| flatMap | étroite | chaque ligne produit 0..n lignes dans la même partition |
| union | étroite | concatène les partitions sans déplacer de données |
| coalesce | étroite | fusionne des partitions voisines sans shuffle |
| reduceByKey | **large** | les valeurs d'une même clé doivent être réunies (shuffle) |
| groupByKey | **large** | idem, sans combiner local |
| distinct | **large** | les doublons peuvent être dans des partitions différentes |
| sortByKey | **large** | tri global : échantillonnage puis redistribution par plages |
| repartition | **large** | redistribue toutes les lignes (shuffle complet) |

## Q4. Persistance

Pour que le lignage coûte cher à rejouer, chaque ligne passe d'abord par un traitement artificiellement lourd (`couteux` : 40 hachages MD5).

**Prédiction** : le second `count()` sur un RDD en cache est nettement plus rapide, car il ne rejoue pas le lignage. `DISK_ONLY` est plus lent que la mémoire, mais plus rapide qu'un recalcul complet si le calcul est coûteux.

| Niveau | 1er count() | 2e count() | Onglet Storage |
|---|---|---|---|
| sans cache | 13.83 s | 14.44 s | — |
| Memory Serialized 1x Replicated | 21.63 s | 9.67 s | mémoire 21.1 Mo, disque 0 o |
| Disk Memory Serialized 1x Replicated | 21.5 s | 9.67 s | mémoire 21.1 Mo, disque 0 o |
| Disk Serialized 1x Replicated | 21.61 s | 10.49 s | mémoire 0 o, disque 21.1 Mo |

Sans cache, les deux `count()` ont le même coût : tout le lignage est rejoué, y compris `couteux`. Avec un niveau de persistance, le premier `count()` calcule **et** stocke les partitions. Le second relit les partitions stockées sans rejouer `couteux`. En PySpark, il reste le coût de désérialisation des objets Python, d'où un gain partiel et non nul. En **MEMORY_ONLY**, une partition qui ne tient pas en mémoire n'est pas stockée : elle est **recalculée à chaque usage**, sans erreur. **MEMORY_AND_DISK** la déverse sur disque. **DISK_ONLY** sérialise tout sur disque.

## Q5. Tolérance aux pannes

| Master | Résultat | Onglet Stages |
|---|---|---|
| local[4] | **job en échec** | `Py4JJavaError` : panne simulée, 1 seule tentative |
| local[4,3] | réussi : count = 500 000 | 1 tâche(s) échouée(s) puis relancée(s) |

Avec `local[4]`, une tâche n'a qu'une tentative en mode local : la panne fait échouer le job. Avec `local[4,3]`, la tâche de la partition 3 échoue à la tentative 0, puis Spark la **relance** et **recalcule la partition à partir du lignage** (`parallelize` → `mapPartitionsWithIndex`). HDFS fonctionne autrement : il ne recalcule rien, il lit une **autre réplique** physique du bloc. Spark protège donc le calcul par le lignage, et HDFS protège les données par la réplication.

## Q6. Broadcast et accumulateurs

Les deux versions donnent le même résultat. La différence est dans l'envoi : un dictionnaire **capturé** dans la fonction est sérialisé avec la fermeture de **chaque tâche**, soit 8 copies ici et des milliers sur un vrai cluster. Une variable **broadcast** est envoyée **une fois par executor**, puis mise en cache et lue avec `b.value`.

| Compteur de « chat » | Valeur |
|---|---|
| Vrai nombre (`filter().count()`) | 926 871 |
| Accumulateur dans une **transformation** (`map`), 2 actions, sans cache | **1 853 742** |
| Accumulateur dans une **action** (`foreach`) | 926 871 |

Dans le `map`, l'accumulateur compte **2 fois** le vrai nombre : le RDD n'est pas en cache, donc chaque action rejoue le `map` et l'incrémente à nouveau. Une tâche relancée après une panne ferait de même. Un accumulateur n'est **fiable que dans une action** (`foreach`), où Spark garantit qu'une tâche réussie ne compte qu'une fois.

