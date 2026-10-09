# Exercice 1 — Anatomie d'une exécution

_Résultats générés par l'exécution réelle du script (PySpark 4.2.0, local, N = 5 000 000 commandes). Les durées dépendent de la machine._

## Q1. Combien de jobs après la construction de `r` ?

**Prédiction (écrite avant exécution)** : 0 job. `filter`, `withColumn` et `groupBy` sont des transformations paresseuses : elles construisent un plan logique sans rien calculer.

**Mesure** : 0 job(s) dans l'onglet Jobs. Prédiction confirmée : rien n'est exécuté tant qu'aucune action n'est appelée.

## Q2. `r.show()` : jobs, stages, tâches

**Prédiction** : 1 job (show), 2 stages séparés par le shuffle du `groupBy`. Stage 1 : 8 tâches (8 partitions de départ). Stage 2 : 8 tâches (`spark.sql.shuffle.partitions = 8`). Avec l'AQE, le second stage peut descendre à 1 tâche car il n'y a que 10 clés : le shuffle pèse quelques Ko.

| Configuration | Jobs | Stages exécutés | Tâches par stage | Durée |
|---|---|---|---|---|
| AQE = true | 2 | 2 | 8 / 1 | 3.77 s |
| AQE = false | 3 | 4 | 8 / 1 / 4 / 3 | 0.46 s |

**Explication.** Le premier stage lit les 8 partitions de `range` (8 tâches) et fait l'agrégation partielle. Le `groupBy` impose un échange (shuffle) : c'est la frontière de stage. Ensuite, deux mécanismes expliquent l'écart avec la prédiction :

- **Avec l'AQE**, le stage de shuffle est exécuté comme un job séparé. Spark mesure alors la taille réelle du shuffle (quelques Ko pour 10 clés) et **fusionne les 8 partitions en 1** (`coalescePartitions`). Le second job n'a qu'une tâche.
- **Sans AQE**, `show()` est un `collectLimit` : Spark lit d'abord 1 partition du shuffle, et comme elle ne suffit pas pour 20 lignes, il relance des jobs sur 4 puis 3 partitions supplémentaires. Au total, on retrouve bien 1 + 4 + 3 = 8 tâches de reduce, soit `spark.sql.shuffle.partitions`. Le stage de map n'est pas recalculé (stages *skipped*), car ses fichiers de shuffle sont réutilisés.

La première mesure inclut aussi le démarrage de la JVM et du code généré, ce qui explique sa durée plus longue.

## Q3. Plan physique

```text
== Physical Plan ==
AdaptiveSparkPlan (7)
+- HashAggregate (6)
   +- Exchange (5)
      +- HashAggregate (4)
         +- Project (3)
            +- Filter (2)
               +- Range (1)


(1) Range
Output [1]: [id#0L]
Arguments: Range (0, 10000000, step=1, splits=Some(8))

(2) Filter
Input [1]: [id#0L]
Condition : ((id#0L % 3) = 0)

(3) Project
Output [1]: [(id#0L % 10) AS k#4L]
Input [1]: [id#0L]

(4) HashAggregate
Input [1]: [k#4L]
Keys [1]: [k#4L]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#45L]
Results [2]: [k#4L, count#46L]

(5) Exchange
Input [2]: [k#4L, count#46L]
Arguments: hashpartitioning(k#4L, 8), ENSURE_REQUIREMENTS, [plan_id=112]

(6) HashAggregate
Input [2]: [k#4L, count#46L]
Keys [1]: [k#4L]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#8L]
Results [2]: [k#4L, count(1)#8L AS count#5L]

(7) AdaptiveSparkPlan
Output [2]: [k#4L, count#5L]
Arguments: isFinalPlan=false
```

On lit le plan de bas en haut : `Range` → `Filter` → `Project` → **`HashAggregate` partiel** → **`Exchange hashpartitioning(k)`** → **`HashAggregate` final**. Il y a deux agrégats parce que Spark compte d'abord localement dans chaque partition, comme un combiner MapReduce. Seuls 10 compteurs par partition traversent le réseau au lieu de 3,3 millions de lignes. L'agrégat final additionne ensuite les comptes partiels de chaque clé.

## Q4. `count()`, `show()`, `collect()`

**Prédiction** : chacune est une action, donc chacune lance au moins un job et recalcule tout depuis `range`, puisque rien n'est en cache.

| Action | Jobs | Stages | Durée |
|---|---|---|---|
| count() | 3 | 3 | 0.53 s |
| show() | 2 | 2 | 0.26 s |
| collect() | 2 | 2 | 0.35 s |

Chaque appel relance un job complet : le lignage est rejoué depuis la source. Pour réutiliser un résultat, il faut `cache()` (voir exercices 2 et 5).

## Q5. Influence du nombre de partitions de départ

**Prédiction** : avec 2 partitions, seuls 2 cœurs sur 4 travaillent. Avec 64 partitions, les tâches sont très courtes et le coût d'ordonnancement devient visible. L'optimum doit se situer autour de 2 à 4 tâches par cœur, soit 8 à 16 ici. Pour que l'effet soit mesurable, cette question utilise 300 millions de lignes au lieu de 10 millions.

| numPartitions | Tâches (stage 1) | Durée totale | Durée médiane d'une tâche |
|---|---|---|---|
| 2 | 2 | 1.7 s | 1471 ms |
| 8 | 8 | 0.85 s | 333 ms |
| 64 | 64 | 1.39 s | 59 ms |

**Règle empirique** : viser 2 à 4 tâches par cœur, avec des tâches d'au moins ~100 ms. En dessous du nombre de cœurs, des cœurs restent inactifs. Au-delà, le coût d'ordonnancement et de lancement des tâches dépasse le calcul utile.

## Q6. Erreurs d'analyse et erreurs de données

| Instruction | Comportement observé |
|---|---|
| `filter(colonne_inexistante > 0)` sans action | **immédiate** : `AnalysisException` à la construction du plan |
| `id / 0` puis `show(3)` (ANSI = true) | **à l'action** : `ArithmeticException` (DIVIDE_BY_ZERO) |
| `cast('int')` puis `show(3)` | aucune erreur (valeurs < 2^31) |

Une colonne inconnue est une **erreur d'analyse** : Spark résout les noms dès la construction du plan, avant toute action. La division par zéro est une **erreur de données** : elle n'existe qu'au moment où les valeurs sont calculées, donc à l'action. Spark 4 active ANSI par défaut (`spark.sql.ansi.enabled = true`) : on obtient une erreur au lieu d'un NULL silencieux. Le cast en `int` ne déborde pas pour des id inférieurs à 10 millions, donc aucune erreur.

## Livrable — DAG annoté de `r`

```text
Job (r.show)
 ├─ Stage 1 ── 8 tâches (1 par partition de range)
 │    Range(0..10M) → Filter(id % 3 = 0) → Project(k = id % 10)
 │    → HashAggregate PARTIEL (count par k, dans chaque partition)
 │    ═════ Exchange hashpartitioning(k, 8) ═════  ← frontière de shuffle
 └─ Stage 2 ── 8 tâches sans AQE (spark.sql.shuffle.partitions)
      │         1 tâche avec AQE (partitions fusionnées)
      HashAggregate FINAL (somme des comptes partiels) → 10 lignes
```

