"""Exercice 2 - RDD, lignage, persistance, tolérance aux pannes, broadcast et accumulateurs."""
import hashlib
import random

from pyspark import StorageLevel, TaskContext

from common import Rapport, mesurer, octets, session


def donnees(sc):
    random.seed(1)
    mots = ["chat"] * 50 + ["chien"] * 20 + [f"mot{i}" for i in range(200)]
    return sc.parallelize([" ".join(random.choices(mots, k=10)) for _ in range(500_000)], 8)


def couteux(ligne):
    """Simule un parsing coûteux (40 hachages MD5 par ligne) pour rendre le lignage cher à rejouer."""
    h = ligne.encode()
    for _ in range(40):
        h = hashlib.md5(h).digest()
    return ligne


def instable(idx, it):
    if idx == 3 and TaskContext.get().attemptNumber() == 0:
        raise RuntimeError("panne simulée")
    return it


def main():
    spark = session()
    sc = spark.sparkContext
    R = Rapport("Exercice 2 — RDD, lignage et variables partagées")
    lignes = donnees(sc)
    paires = lignes.flatMap(lambda l: l.split()).map(lambda m: (m, 1))

    # Q1 - WordCount
    wc = paires.reduceByKey(lambda a, b: a + b)
    top5 = wc.takeOrdered(5, key=lambda kv: -kv[1])
    R("## Q1. WordCount avec `reduceByKey`", "")
    R.code("wc = lignes.flatMap(lambda l: l.split()).map(lambda m: (m, 1)).reduceByKey(lambda a, b: a + b)\n"
           "wc.takeOrdered(5, key=lambda kv: -kv[1])", "python")
    R.tableau(["Mot", "Occurrences"], [[m, f"{c:,}".replace(",", " ")] for m, c in top5])
    total = 500_000 * 10
    attendu = f"{total * 50 / 270:,.0f}".replace(",", " ")
    R(f"Contrôle : 5 000 000 mots tirés ; « chat » a un poids de 50/270, soit environ {attendu} attendus.", "")

    # Q2 - reduceByKey vs groupByKey
    R("## Q2. `reduceByKey` contre `groupByKey`", "",
      "**Prédiction** : `reduceByKey` écrit beaucoup moins de données au shuffle. Il combine (mot, 1) "
      "localement dans chaque partition, donc au plus 272 paires par partition traversent le réseau. "
      "`groupByKey` envoie les 5 millions de paires.", "")
    m_r = mesurer(spark, lambda: paires.reduceByKey(lambda a, b: a + b).collect())
    m_g = mesurer(spark, lambda: paires.groupByKey().mapValues(sum).collect())
    R.tableau(["Version", "Shuffle Write", "Shuffle Read", "Durée"],
              [["reduceByKey", octets(m_r["shuffle_write"]), octets(m_r["shuffle_read"]), f"{m_r['duree_s']} s"],
               ["groupByKey + mapValues(sum)", octets(m_g["shuffle_write"]), octets(m_g["shuffle_read"]), f"{m_g['duree_s']} s"]])
    ratio = m_g["shuffle_write"] / max(m_r["shuffle_write"], 1)
    R(f"`groupByKey` écrit **{ratio:.0f} fois plus** de données au shuffle. Le **combiner côté map** de "
      "`reduceByKey` additionne les 1 de chaque mot dans la partition avant l'échange : seuls des "
      "sous-totaux traversent le réseau.", "",
      "**Écart avec la prédiction** : l'écart attendu était bien plus grand. Il est réduit pour deux "
      "raisons. D'abord, en PySpark, `groupByKey` regroupe déjà les valeurs en listes dans chaque "
      "partition avant l'échange. Ensuite, ces listes de 1 très répétitives sont sérialisées par lots "
      "et compressées (lz4), ce qui réduit fortement leur taille. Avec des valeurs variées (montants, "
      "identifiants) ou en Scala, la différence serait proportionnelle au nombre de paires. Les durées "
      "proches, environ 20 s, sont dominées par les échanges entre la JVM et les workers Python, qui "
      "sont lents sous Windows.", "")

    # Q3 - lignage
    R("## Q3. Lignage (`toDebugString`)", "")
    for nom, rdd in [("reduceByKey", paires.reduceByKey(lambda a, b: a + b)),
                     ("groupByKey", paires.groupByKey().mapValues(sum))]:
        R(f"**{nom}**")
        R.code(rdd.toDebugString().decode(), "text")
    R("Le changement d'indentation `+-(8)` marque le **ShuffledRDD**, c'est-à-dire la frontière de "
      "stage. Au-dessus, le stage reduce ; en dessous, le stage map (`ParallelCollectionRDD` → "
      "`PythonRDD` pour flatMap + map). C'est cette recette que Spark rejoue pour recalculer une "
      "partition perdue.", "")
    R.tableau(["Transformation", "Type", "Pourquoi"], [
        ["map", "étroite", "1 partition d'entrée → 1 partition de sortie"],
        ["filter", "étroite", "garde ou jette des lignes sur place"],
        ["flatMap", "étroite", "chaque ligne produit 0..n lignes dans la même partition"],
        ["union", "étroite", "concatène les partitions sans déplacer de données"],
        ["coalesce", "étroite", "fusionne des partitions voisines sans shuffle"],
        ["reduceByKey", "**large**", "les valeurs d'une même clé doivent être réunies (shuffle)"],
        ["groupByKey", "**large**", "idem, sans combiner local"],
        ["distinct", "**large**", "les doublons peuvent être dans des partitions différentes"],
        ["sortByKey", "**large**", "tri global : échantillonnage puis redistribution par plages"],
        ["repartition", "**large**", "redistribue toutes les lignes (shuffle complet)"],
    ])

    # Q4 - persistance
    R("## Q4. Persistance", "",
      "Pour que le lignage coûte cher à rejouer, chaque ligne passe d'abord par un traitement "
      "artificiellement lourd (`couteux` : 40 hachages MD5).", "",
      "**Prédiction** : le second `count()` sur un RDD en cache est nettement plus rapide, car il ne "
      "rejoue pas le lignage. `DISK_ONLY` est plus lent que la mémoire, mais plus rapide qu'un recalcul complet "
      "si le calcul est coûteux.", "")
    res = []
    for niveau in [None, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_AND_DISK, StorageLevel.DISK_ONLY]:
        rdd = lignes.map(couteux).flatMap(lambda l: l.split()).map(lambda m: (m, 1))
        if niveau:
            rdd.persist(niveau)
        a = mesurer(spark, rdd.count)
        b = mesurer(spark, rdd.count)
        stockage = ""
        if niveau:
            import json, urllib.request  # noqa: E401
            url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/storage/rdd"
            infos = json.load(urllib.request.urlopen(url))
            if infos:
                i = infos[-1]
                stockage = f"mémoire {octets(i['memoryUsed'])}, disque {octets(i['diskUsed'])}"
        res.append([str(niveau).split("(")[0] if niveau else "sans cache",
                    f"{a['duree_s']} s", f"{b['duree_s']} s", stockage or "—"])
        rdd.unpersist(blocking=True)
    R.tableau(["Niveau", "1er count()", "2e count()", "Onglet Storage"], res)
    R("Sans cache, les deux `count()` ont le même coût : tout le lignage est rejoué, y compris "
      "`couteux`. Avec un niveau de persistance, le premier `count()` calcule **et** stocke les "
      "partitions. Le second relit les partitions stockées sans rejouer `couteux`. En PySpark, il "
      "reste le coût de désérialisation des objets Python, d'où un gain partiel et non nul. En **MEMORY_ONLY**, une partition qui ne tient "
      "pas en mémoire n'est pas stockée : elle est **recalculée à chaque usage**, sans erreur. "
      "**MEMORY_AND_DISK** la déverse sur disque. **DISK_ONLY** sérialise tout sur disque.", "")

    # Q5 - pannes
    R("## Q5. Tolérance aux pannes", "")
    spark.stop()
    lignes_res = []
    for master in ["local[4]", "local[4,3]"]:
        spark = session(master)
        sc = spark.sparkContext
        rdd = donnees(sc)
        try:
            m = mesurer(spark, lambda: rdd.mapPartitionsWithIndex(instable).count())
            echecs = sum(s["echecs"] for s in m["stages"])
            lignes_res.append([master, f"réussi : count = {m['resultat']:,}".replace(",", " "),
                               f"{echecs} tâche(s) échouée(s) puis relancée(s)"])
        except Exception as e:  # noqa: BLE001
            lignes_res.append([master, "**job en échec**", f"`{type(e).__name__}` : panne simulée, 1 seule tentative"])
        spark.stop()
    R.tableau(["Master", "Résultat", "Onglet Stages"], lignes_res)
    R("Avec `local[4]`, une tâche n'a qu'une tentative en mode local : la panne fait échouer le job. "
      "Avec `local[4,3]`, la tâche de la partition 3 échoue à la tentative 0, puis Spark la **relance** "
      "et **recalcule la partition à partir du lignage** (`parallelize` → `mapPartitionsWithIndex`). "
      "HDFS fonctionne autrement : il ne recalcule rien, il lit une **autre réplique** physique du bloc. "
      "Spark protège donc le calcul par le lignage, et HDFS protège les données par la réplication.", "")

    # Q6 - broadcast / accumulateur
    spark = session()
    sc = spark.sparkContext
    lignes = donnees(sc)
    mots = lignes.flatMap(lambda l: l.split())
    categorie = {"chat": "animal", "chien": "animal"}
    capture = mots.map(lambda m: (m, categorie.get(m, "autre")))
    b = sc.broadcast(categorie)
    diffuse = mots.map(lambda m: (m, b.value.get(m, "autre")))
    assert capture.countByValue() == diffuse.countByValue()
    vrai_chat = mots.filter(lambda m: m == "chat").count()

    acc_t = sc.accumulator(0)

    def compte(m):
        if m == "chat":
            acc_t.add(1)
        return m
    marque = mots.map(compte)
    marque.count()
    marque.count()
    acc_a = sc.accumulator(0)
    mots.foreach(lambda m: acc_a.add(1) if m == "chat" else None)

    R("## Q6. Broadcast et accumulateurs", "")
    R("Les deux versions donnent le même résultat. La différence est dans l'envoi : un dictionnaire "
      "**capturé** dans la fonction est sérialisé avec la fermeture de **chaque tâche**, soit 8 copies "
      "ici et des milliers sur un vrai cluster. Une variable **broadcast** est envoyée **une fois par "
      "executor**, puis mise en cache et lue avec `b.value`.", "")
    R.tableau(["Compteur de « chat »", "Valeur"], [
        ["Vrai nombre (`filter().count()`)", f"{vrai_chat:,}".replace(",", " ")],
        ["Accumulateur dans une **transformation** (`map`), 2 actions, sans cache",
         f"**{acc_t.value:,}**".replace(",", " ")],
        ["Accumulateur dans une **action** (`foreach`)", f"{acc_a.value:,}".replace(",", " ")],
    ])
    R(f"Dans le `map`, l'accumulateur compte **{acc_t.value / vrai_chat:.0f} fois** le vrai nombre : le "
      "RDD n'est pas en cache, donc chaque action rejoue le `map` et l'incrémente à nouveau. Une tâche "
      "relancée après une panne ferait de même. Un accumulateur n'est **fiable que dans une action** "
      "(`foreach`), où Spark garantit qu'une tâche réussie ne compte qu'une fois.", "")
    R.sauver("ex2_rdd.md")
    spark.stop()


if __name__ == "__main__":
    main()
