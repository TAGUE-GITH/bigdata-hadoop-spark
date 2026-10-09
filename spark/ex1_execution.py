"""Exercice 1 - Anatomie d'une exécution : jobs, stages, tâches, plan, AQE, erreurs paresseuses."""
import contextlib
import io

from pyspark.sql import functions as F

from common import Rapport, mesurer, session


def plan(df, mode="formatted") -> str:
    tampon = io.StringIO()
    with contextlib.redirect_stdout(tampon):
        df.explain(mode)
    return tampon.getvalue()


def requete(spark, partitions=8, lignes=10_000_000):
    df = spark.range(0, lignes, numPartitions=partitions)
    return df.filter(F.col("id") % 3 == 0).withColumn("k", F.col("id") % 10).groupBy("k").count()


def main():
    spark = session()
    R = Rapport("Exercice 1 — Anatomie d'une exécution")

    # Q1 - transformations seules
    R("## Q1. Combien de jobs après la construction de `r` ?", "",
      "**Prédiction (écrite avant exécution)** : 0 job. `filter`, `withColumn` et `groupBy` sont des "
      "transformations paresseuses : elles construisent un plan logique sans rien calculer.", "")
    m = mesurer(spark, lambda: requete(spark))
    r = m["resultat"]
    R(f"**Mesure** : {m['jobs']} job(s) dans l'onglet Jobs. Prédiction confirmée : rien n'est exécuté "
      "tant qu'aucune action n'est appelée.", "")

    # Q2 - show avec et sans AQE
    R("## Q2. `r.show()` : jobs, stages, tâches", "",
      "**Prédiction** : 1 job (show), 2 stages séparés par le shuffle du `groupBy`. Stage 1 : 8 tâches "
      "(8 partitions de départ). Stage 2 : 8 tâches (`spark.sql.shuffle.partitions = 8`). Avec l'AQE, "
      "le second stage peut descendre à 1 tâche car il n'y a que 10 clés : le shuffle pèse quelques Ko.", "")
    lignes = []
    for aqe in ["true", "false"]:
        spark.conf.set("spark.sql.adaptive.enabled", aqe)
        m = mesurer(spark, lambda: requete(spark).show())
        lignes.append([f"AQE = {aqe}", m["jobs"], m["nb_stages"],
                       " / ".join(str(s["taches"]) for s in m["stages"]), f"{m['duree_s']} s"])
    R.tableau(["Configuration", "Jobs", "Stages exécutés", "Tâches par stage", "Durée"], lignes)
    R("**Explication.** Le premier stage lit les 8 partitions de `range` (8 tâches) et fait l'agrégation "
      "partielle. Le `groupBy` impose un échange (shuffle) : c'est la frontière de stage. Ensuite, deux "
      "mécanismes expliquent l'écart avec la prédiction :", "",
      "- **Avec l'AQE**, le stage de shuffle est exécuté comme un job séparé. Spark mesure alors la taille "
      "réelle du shuffle (quelques Ko pour 10 clés) et **fusionne les 8 partitions en 1** "
      "(`coalescePartitions`). Le second job n'a qu'une tâche.",
      "- **Sans AQE**, `show()` est un `collectLimit` : Spark lit d'abord 1 partition du shuffle, et "
      "comme elle ne suffit pas pour 20 lignes, il relance des jobs sur 4 puis 3 partitions "
      "supplémentaires. Au total, on retrouve bien 1 + 4 + 3 = 8 tâches de reduce, soit "
      "`spark.sql.shuffle.partitions`. Le stage de map n'est pas recalculé (stages *skipped*), car "
      "ses fichiers de shuffle sont réutilisés.", "",
      "La première mesure inclut aussi le démarrage de la JVM et du code généré, ce qui explique sa durée plus longue.", "")
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    # Q3 - plan
    R("## Q3. Plan physique", "")
    R.code(plan(r), "text")
    R("On lit le plan de bas en haut : `Range` → `Filter` → `Project` → **`HashAggregate` partiel** → "
      "**`Exchange hashpartitioning(k)`** → **`HashAggregate` final**. Il y a deux agrégats parce que Spark "
      "compte d'abord localement dans chaque partition, comme un combiner MapReduce. Seuls 10 compteurs par "
      "partition traversent le réseau au lieu de 3,3 millions de lignes. L'agrégat final additionne ensuite "
      "les comptes partiels de chaque clé.", "")

    # Q4 - count / show / collect
    R("## Q4. `count()`, `show()`, `collect()`", "",
      "**Prédiction** : chacune est une action, donc chacune lance au moins un job et recalcule tout depuis "
      "`range`, puisque rien n'est en cache.", "")
    lignes = []
    for nom, action in [("count()", r.count), ("show()", r.show), ("collect()", r.collect)]:
        m = mesurer(spark, action)
        lignes.append([nom, m["jobs"], m["nb_stages"], f"{m['duree_s']} s"])
    R.tableau(["Action", "Jobs", "Stages", "Durée"], lignes)
    R("Chaque appel relance un job complet : le lignage est rejoué depuis la source. Pour réutiliser un "
      "résultat, il faut `cache()` (voir exercices 2 et 5).", "")

    # Q5 - nombre de partitions
    R("## Q5. Influence du nombre de partitions de départ", "",
      "**Prédiction** : avec 2 partitions, seuls 2 cœurs sur 4 travaillent. Avec 64 partitions, les "
      "tâches sont très courtes et le coût d'ordonnancement devient visible. L'optimum doit se situer "
      "autour de 2 à 4 tâches par cœur, soit 8 à 16 ici. Pour que l'effet soit mesurable, cette "
      "question utilise 300 millions de lignes au lieu de 10 millions.", "")
    lignes = []
    for p in [2, 8, 64]:
        mesurer(spark, lambda: requete(spark, p, 300_000_000).collect())  # préchauffage JVM
        m = mesurer(spark, lambda: requete(spark, p, 300_000_000).collect(), details_taches=True)
        st = m["stages"][0]
        lignes.append([p, st["taches"], f"{m['duree_s']} s", f"{st.get('tache_mediane_ms', 0):.0f} ms"])
    R.tableau(["numPartitions", "Tâches (stage 1)", "Durée totale", "Durée médiane d'une tâche"], lignes)
    R("**Règle empirique** : viser 2 à 4 tâches par cœur, avec des tâches d'au moins ~100 ms. En dessous "
      "du nombre de cœurs, des cœurs restent inactifs. Au-delà, le coût d'ordonnancement et de "
      "lancement des tâches dépasse le calcul utile.", "")

    # Q6 - erreurs
    R("## Q6. Erreurs d'analyse et erreurs de données", "")
    df = spark.range(0, 10_000_000, numPartitions=8)
    lignes = []
    try:
        df.filter(F.col("colonne_inexistante") > 0)
        lignes.append(["`filter(colonne_inexistante > 0)` sans action", "aucune erreur"])
    except Exception as e:  # noqa: BLE001
        lignes.append(["`filter(colonne_inexistante > 0)` sans action",
                       f"**immédiate** : `{type(e).__name__}` à la construction du plan"])
    ansi = spark.conf.get("spark.sql.ansi.enabled")
    try:
        df.select((F.col("id") / 0).alias("x")).show(3)
        lignes.append([f"`id / 0` puis `show(3)` (ANSI = {ansi})", "aucune erreur, résultat NULL"])
    except Exception as e:  # noqa: BLE001
        lignes.append([f"`id / 0` puis `show(3)` (ANSI = {ansi})",
                       f"**à l'action** : `{type(e).__name__}` (DIVIDE_BY_ZERO)"])
    try:
        df.select(F.col("id").cast("int")).show(3)
        lignes.append(["`cast('int')` puis `show(3)`", "aucune erreur (valeurs < 2^31)"])
    except Exception as e:  # noqa: BLE001
        lignes.append(["`cast('int')` puis `show(3)`", f"erreur : {type(e).__name__}"])
    R.tableau(["Instruction", "Comportement observé"], lignes)
    R(f"Une colonne inconnue est une **erreur d'analyse** : Spark résout les noms dès la construction du "
      f"plan, avant toute action. La division par zéro est une **erreur de données** : elle n'existe qu'au "
      f"moment où les valeurs sont calculées, donc à l'action. Spark 4 active ANSI par défaut "
      f"(`spark.sql.ansi.enabled = {ansi}`) : on obtient une erreur au lieu d'un NULL silencieux. Le cast "
      f"en `int` ne déborde pas pour des id inférieurs à 10 millions, donc aucune erreur.", "")

    R("## Livrable — DAG annoté de `r`", "")
    R.code("""Job (r.show)
 ├─ Stage 1 ── 8 tâches (1 par partition de range)
 │    Range(0..10M) → Filter(id % 3 = 0) → Project(k = id % 10)
 │    → HashAggregate PARTIEL (count par k, dans chaque partition)
 │    ═════ Exchange hashpartitioning(k, 8) ═════  ← frontière de shuffle
 └─ Stage 2 ── 8 tâches sans AQE (spark.sql.shuffle.partitions)
      │         1 tâche avec AQE (partitions fusionnées)
      HashAggregate FINAL (somme des comptes partiels) → 10 lignes""", "text")
    R.sauver("ex1_execution.md")
    spark.stop()


if __name__ == "__main__":
    main()
