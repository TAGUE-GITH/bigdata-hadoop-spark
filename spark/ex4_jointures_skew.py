"""Exercice 4 - Jointures, skew et partitionnement."""
import contextlib
import io
import re

from pyspark.sql import functions as F

from common import Rapport, charger, mesurer, octets, session, stage_le_plus_lent


def plan(df, mode="simple") -> str:
    t = io.StringIO()
    with contextlib.redirect_stdout(t):
        df.explain(mode)
    return t.getvalue()


def plan_final(df) -> str:
    """Plan AQE final (après exécution), où apparaissent les décisions adaptatives comme skew=true."""
    return df._jdf.queryExecution().executedPlan().toString()


def ca_par_famille(commandes, produits):
    return (commandes.join(produits, "id_produit")
            .groupBy("famille").agg(F.round(F.sum(F.col("quantite") * F.col("prix_unitaire")), 2).alias("ca")))


def ligne_mesure(nom, m, p):
    st = stage_le_plus_lent(m)
    ratio = st.get("tache_max_ms", 0) / max(st.get("tache_mediane_ms", 1), 1)
    jointure = re.search(r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin)", p)
    return [nom, jointure.group(1) if jointure else "—", p.count("Exchange"), f"{m['duree_s']} s",
            octets(m["shuffle_write"]), f"{st.get('tache_max_ms', 0):,} ms".replace(",", " "),
            f"{st.get('tache_mediane_ms', 0):,.0f} ms".replace(",", " "), f"{ratio:.1f}"]


ENTETES = ["Stratégie", "Jointure", "Exchange", "Durée", "Shuffle write", "Tâche max", "Tâche médiane", "Max / médiane"]


def main():
    spark = session()
    commandes, produits = charger(spark)
    commandes.cache().count()
    produits.cache().count()
    R = Rapport("Exercice 4 — Jointures, skew et partitionnement")
    comparatif = []

    # Q1 - jointure subie
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    R("## Q1. Jointure subie (broadcast et AQE désactivés)", "",
      "**Prédiction** : SortMergeJoin, avec 3 Exchange (un par côté de la jointure, plus un pour le "
      "`groupBy`). Dans le stage de jointure, une tâche reçoit toute la clé P000, soit environ 60 % des "
      "lignes, et dure bien plus longtemps que les autres.", "")
    df = ca_par_famille(commandes, produits)
    p1 = plan(df)
    m = mesurer(spark, df.collect, details_taches=True)
    comparatif.append(ligne_mesure("Jointure subie (SortMergeJoin)", m, p1))
    R.code(p1, "text")
    R.tableau(["famille", "CA"], [[r["famille"], f"{r['ca']:,.2f}".replace(",", " ")] for r in sorted(m["resultat"])])
    st = stage_le_plus_lent(m)
    R("Le CA d'Éclairage écrase les autres familles : P000 (id 0, donc 0 % 4 = Éclairage) porte 60 % des "
      "commandes. Le déséquilibre métier se retrouve directement dans le déséquilibre technique.", "")
    R(f"Plan : **SortMergeJoin**, **{p1.count('Exchange')} Exchange**. Dans le stage de jointure, la "
      f"tâche la plus longue dure {st['tache_max_ms']} ms contre {st['tache_mediane_ms']:.0f} ms pour la "
      "médiane. C'est la partition qui reçoit le hachage de `P000` : toutes les lignes d'une même clé "
      "vont dans la même partition.", "")

    # Q2 - broadcast
    R("## Q2. Jointure broadcast", "")
    df_b = commandes.join(F.broadcast(produits), "id_produit").groupBy("famille").agg(
        F.round(F.sum(F.col("quantite") * F.col("prix_unitaire")), 2).alias("ca"))
    p2 = plan(df_b)
    m = mesurer(spark, df_b.collect, details_taches=True)
    comparatif.append(ligne_mesure("Broadcast forcé", m, p2))
    R.code(p2, "text")
    R(f"Plan : **BroadcastHashJoin**, {p2.count('Exchange')} Exchange (`BroadcastExchange` de la petite "
      "table et l'échange du `groupBy`). Les 5 M commandes **ne sont plus redistribuées** pour la "
      "jointure : chaque tâche joint ses lignes avec une copie locale des 1 000 produits. Le skew "
      "disparaît, car aucune partition ne regroupe P000. Le seuil automatique "
      "`spark.sql.autoBroadcastJoinThreshold` vaut **10 Mo** par défaut. Si la « petite » table "
      "faisait en réalité 5 Go, le broadcast automatique ne s'appliquerait pas. Un broadcast forcé "
      "devrait rassembler 5 Go sur le Driver puis les copier dans chaque executor, avec un risque d'OOM "
      "et de saturation réseau.", "")

    # Q3 - diagnostic
    R("## Q3. Diagnostiquer le skew", "")
    dist = commandes.groupBy("id_produit").count().orderBy(F.desc("count")).limit(5).collect()
    total = commandes.count()
    R.tableau(["id_produit", "commandes", "part"],
              [[r["id_produit"], f"{r['count']:,}".replace(",", " "), f"{100 * r['count'] / total:.1f} %"] for r in dist])
    ratio = st["tache_max_ms"] / max(st["tache_mediane_ms"], 1)
    R(f"P000 concentre **{100 * dist[0]['count'] / total:.1f} %** des commandes, contre environ 0,04 % "
      f"pour les autres produits. Sur la jointure subie, le ratio entre la tâche maximale et la tâche "
      f"médiane vaut **{ratio:.1f}**. Un ratio supérieur à 3 à 5 est le signal d'un skew : le stage dure "
      "aussi longtemps que sa tâche la plus lente, pendant que les autres cœurs attendent.", "")

    # Q4 - trois corrections
    R("## Q4. Corriger le skew (broadcast désactivé)", "")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # Seuils abaissés pour qu'un jeu de 5 M lignes en local soit jugé « déséquilibré »
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "8MB")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "4MB")
    df_a = ca_par_famille(commandes, produits)
    m = mesurer(spark, df_a.collect, details_taches=True)
    pf = plan_final(df_a)
    comparatif.append(ligne_mesure("(a) AQE skewJoin", m, pf))
    skew = "skew=true" in pf
    R(f"**(a) AQE + `skewJoin`**. Plan final AQE : `SortMergeJoin(skew={str(skew).lower()})`. "
      + ("Spark a détecté la partition P000 comme déséquilibrée et l'a découpée en plusieurs tâches. "
         "La petite table est dupliquée en face de chaque morceau." if skew else
         "La partition n'a pas dépassé le seuil de détection : aucune découpe.")
      + " Les seuils `skewedPartitionThresholdInBytes` (8 Mo) et `advisoryPartitionSizeInBytes` (4 Mo) "
        "ont été abaissés pour ce volume local. Les valeurs par défaut (256 Mo / 64 Mo) visent des données "
        "de production.", "")

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    cmd_salt = commandes.withColumn("salt", (F.rand(42) * 8).cast("int"))
    prod_salt = produits.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(7))))
    df_s = (cmd_salt.join(prod_salt, ["id_produit", "salt"]).groupBy("famille")
            .agg(F.round(F.sum(F.col("quantite") * F.col("prix_unitaire")), 2).alias("ca")))
    m = mesurer(spark, df_s.collect, details_taches=True)
    comparatif.append(ligne_mesure("(b) Salage x8", m, plan(df_s)))
    R("**(b) Salage**. On ajoute à `commandes` une colonne `salt` aléatoire de 0 à 7 et on duplique "
      "`produits` 8 fois. La jointure se fait sur `(id_produit, salt)` : P000 est réparti en 8 clés, "
      "donc sur 8 partitions. La dimension pèse 8 fois plus lourd, et le code est moins lisible.", "")

    chaud = commandes.filter(F.col("id_produit") == "P000")
    froid = commandes.filter(F.col("id_produit") != "P000")
    jointure = (chaud.join(F.broadcast(produits.filter(F.col("id_produit") == "P000")), "id_produit")
                .unionByName(froid.join(produits, "id_produit")))
    df_c = jointure.groupBy("famille").agg(F.round(F.sum(F.col("quantite") * F.col("prix_unitaire")), 2).alias("ca"))
    m = mesurer(spark, df_c.collect, details_taches=True)
    comparatif.append(ligne_mesure("(c) Clé chaude à part", m, plan(df_c)))
    R("**(c) Clé chaude traitée à part**. Les commandes P000 sont jointes en broadcast avec une "
      "dimension d'une ligne, et le reste par une jointure classique, désormais équilibrée. On termine "
      "par `unionByName`.", "")

    for d in (df_a, df_s, df_c):
        assert sorted(d.collect()) == sorted(ca_par_famille(commandes, produits).collect())
    R("Contrôle : les trois corrections donnent **exactement le même CA par famille** que la jointure subie.", "")
    R("### Livrable — comparatif des stratégies", "")
    R.tableau(ENTETES, comparatif)
    R("Lecture : la colonne *Max / médiane* mesure le déséquilibre du stage le plus lent.", "",
      "- **Broadcast** : la plus efficace quand la dimension est petite, puisqu'il n'y a plus de "
      "shuffle des faits.",
      "- **AQE** : la plus simple, il suffit d'une configuration, mais la correction dépend des seuils.",
      "- **Salage** : la plus générale, car elle marche aussi entre deux grandes tables, mais elle "
      "complique le code et multiplie la dimension.",
      "- **Clé chaude à part** : efficace et explicite, mais il faut connaître les clés chaudes à l'avance.", "")

    # Q5 - shuffle partitions
    R("## Q5. Nombre de partitions après shuffle", "")
    res = []
    for aqe in ["false", "true"]:
        spark.conf.set("spark.sql.adaptive.enabled", aqe)
        for n in [2, 8, 200]:
            spark.conf.set("spark.sql.shuffle.partitions", n)
            g = commandes.groupBy("pays", "id_produit").agg(F.sum("quantite").alias("q"))
            m = mesurer(spark, g.collect)
            reduce = [s["taches"] for s in m["stages"] if s["shuffle_read"] > 0]
            res.append([f"AQE {aqe}", n, sum(reduce), f"{m['duree_s']} s"])
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    R.tableau(["Configuration", "shuffle.partitions", "Tâches de reduce", "Durée"], res)
    R("Le résultat ne compte qu'environ 6 000 groupes (6 pays × 1 000 produits), soit quelques "
      "centaines de Ko. Avec 200 partitions sans AQE, Spark lance 200 tâches qui traitent chacune "
      "presque rien : le coût d'ordonnancement, de lancement et d'ouverture des blocs de shuffle "
      "domine. Avec l'AQE, `coalescePartitions` fusionne ces micro-partitions en quelques tâches, d'où "
      "un nombre de tâches de reduce indépendant du réglage.", "")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    # Q6 - partitionnement
    R("## Q6. Contrôler le partitionnement", "")
    base = commandes.select("pays", "date_commande")
    variantes = [("repartition(8)", base.repartition(8)), ("repartition(8, 'pays')", base.repartition(8, "pays")),
                 ("repartitionByRange(8, 'date_commande')", base.repartitionByRange(8, "date_commande")),
                 ("coalesce(2)", base.coalesce(2))]
    res = []
    for nom, d in variantes:
        tailles = [r["count"] for r in d.groupBy(F.spark_partition_id().alias("p")).count().collect()]
        res.append([f"`{nom}`", "oui" if "Exchange" in plan(d) else "non", len(tailles),
                    f"{min(tailles):,} → {max(tailles):,}".replace(",", " ")])
    R.tableau(["Opération", "Shuffle", "Partitions non vides", "Lignes par partition (min → max)"], res)
    R("`repartition(8)` équilibre parfaitement, au prix d'un shuffle complet. `repartition(8, 'pays')` "
      "hache la valeur du pays : il n'y a que **6 clés distinctes** (5 pays + NULL), donc au plus 6 "
      "partitions non vides. Deux pays peuvent même tomber dans la même partition par collision de "
      "hachage. `repartitionByRange` découpe par plages de dates de taille proche, en s'appuyant sur un "
      "échantillonnage. `coalesce(2)` fusionne des partitions existantes **sans shuffle** : c'est peu "
      "coûteux, mais rien ne garantit l'équilibre.", "")

    # Q7 - semi / anti / non égale
    R("## Q7. Jointures semi, anti et non égale", "")
    echantillon = commandes.limit(1000)
    jamais = produits.join(echantillon, "id_produit", "left_anti").count()
    deja = produits.join(echantillon, "id_produit", "left_semi").count()
    R(f"Sur `commandes.limit(1000)` : **{deja}** produits commandés (`left_semi`) et **{jamais}** jamais "
      f"commandés (`left_anti`), soit {deja + jamais} = 1 000 produits.", "",
      "`left_semi` et `left_anti` renvoient uniquement les colonnes de gauche, **sans dupliquer** une "
      "ligne qui a plusieurs correspondances. Spark peut arrêter la recherche dès la première "
      "correspondance. Un `left join` suivi d'un `filter(isNull)` matérialise toutes les "
      "correspondances avant d'en jeter la plupart.", "")
    a = spark.range(5).withColumnRenamed("id", "x")
    b = spark.range(5).withColumnRenamed("id", "y")
    pn = plan(a.join(b, a.x < b.y))
    op = re.search(r"(BroadcastNestedLoopJoin|CartesianProduct)", pn)
    R(f"Jointure non égale `a.x < b.y` : opérateur **{op.group(1) if op else '?'}**. Sans clé d'égalité, "
      "on ne peut ni hacher ni trier par clé : chaque ligne de `a` est comparée à **toutes** les lignes "
      "de `b`, soit un coût en O(n × m). Avec deux tables d'un million de lignes, cela fait 10^12 "
      "comparaisons. On obtient ici un `CartesianProduct` parce que le broadcast automatique a été "
      "désactivé en Q1. Avec le broadcast actif, Spark choisit un `BroadcastNestedLoopJoin`, de même "
      "complexité, mais sans shuffle.", "")
    R.code(pn, "text")
    R.sauver("ex4_jointures_skew.md")
    spark.stop()


if __name__ == "__main__":
    main()
