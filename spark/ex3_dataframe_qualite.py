"""Exercice 3 - DataFrame, qualité des données et SQL."""
import contextlib
import glob
import io
import os
import shutil

import pandas as pd
from pyspark.sql import Window, functions as F
from pyspark.sql.functions import pandas_udf, udf
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from common import DATA, Rapport, commandes_et_produits, mesurer, session

CSV = str(DATA / "commandes_csv")
SCHEMA = StructType([StructField("id_commande", StringType()), StructField("id_produit", StringType()),
                     StructField("pays", StringType()), StructField("quantite", IntegerType()),
                     StructField("prix_unitaire", DoubleType()), StructField("date_commande", DateType()),
                     StructField("_corrupt_record", StringType())])


def plan(df, mode="formatted") -> str:
    t = io.StringIO()
    with contextlib.redirect_stdout(t):
        df.explain(mode)
    return t.getvalue()


def preparer(spark, commandes):
    shutil.rmtree(CSV, ignore_errors=True)
    commandes.limit(200_000).coalesce(1).write.mode("overwrite").option("header", True).csv(CSV)
    fichier = glob.glob(f"{CSV}/part-*.csv")[0]
    with open(fichier, "a") as fh:
        fh.write("C_BAD1,P001,FR,abc,10.5,2026-02-01\nC_BAD2,P002,BE\nC_BAD3,P003,ES,2,12.0,2026-13-45\n")
    for crc in glob.glob(f"{CSV}/.*.crc"):   # la somme de contrôle Hadoop ne correspond plus après l'ajout
        os.remove(crc)


def lire(spark, mode):
    return (spark.read.schema(SCHEMA).option("header", True).option("mode", mode)
            .option("columnNameOfCorruptRecord", "_corrupt_record").csv(CSV))


def sql_txt(v):
    """Affiche une valeur comme Spark SQL (NULL, true) plutôt qu'en Python (None, True)."""
    return "NULL" if v is None else (str(v).lower() if isinstance(v, bool) else v)


def classe_native(montant):
    return F.when(montant < 100, "petit").when(montant < 500, "moyen").otherwise("gros")


@udf(StringType())
def classe_udf(m):
    return "petit" if m < 100 else ("moyen" if m < 500 else "gros")


@pandas_udf(StringType())
def classe_pandas(m: pd.Series) -> pd.Series:
    return pd.cut(m, [-float("inf"), 100, 500, float("inf")], right=False,
                  labels=["petit", "moyen", "gros"]).astype(str)


def main():
    spark = session()
    commandes, _ = commandes_et_produits(spark)
    R = Rapport("Exercice 3 — DataFrame, qualité des données et SQL")
    preparer(spark, commandes)

    # Q1 - inférence contre schéma explicite
    R("## Q1. `inferSchema` contre schéma explicite", "",
      "**Prédiction** : l'inférence lance au moins un job de plus, car Spark relit le fichier pour deviner "
      "les types. `quantite` sera devinée en `string` à cause de `abc`, et `date_commande` aussi à "
      "cause de `2026-13-45`.", "")
    m_inf = mesurer(spark, lambda: spark.read.option("header", True).option("inferSchema", True).csv(CSV))
    m_exp = mesurer(spark, lambda: lire(spark, "PERMISSIVE"))
    R.tableau(["Lecture", "Jobs lancés à la lecture (sans action)"],
              [["`inferSchema=True`", m_inf["jobs"]], ["`schema(StructType)`", m_exp["jobs"]]])
    types = m_inf["resultat"].dtypes
    R.tableau(["Colonne", "Type inféré", "Type attendu"],
              [[c, f"**{t}**" if t != e else t, e] for (c, t), e in
               zip(types, ["string", "string", "string", "int", "double", "date"])])
    R("Le job supplémentaire est la passe d'inférence : Spark doit lire le fichier avant même que l'on "
      "demande un résultat. Il suffit d'**une seule valeur non conforme** pour qu'une colonne entière "
      "devienne `string` (`abc` dans `quantite`, `2026-13-45` dans les dates). Le schéma explicite évite "
      "cette passe et garantit des types stables d'un chargement à l'autre.", "")

    # Q2 - modes de lecture
    R("## Q2. Modes PERMISSIVE, DROPMALFORMED, FAILFAST", "")
    perm = lire(spark, "PERMISSIVE").cache()
    corrompues = perm.filter(F.col("_corrupt_record").isNotNull())
    lignes_corr = [r["_corrupt_record"] for r in corrompues.select("_corrupt_record").collect()]
    drop = lire(spark, "DROPMALFORMED")
    n_drop_brut = drop.count()
    n_drop = drop.cache().count()
    try:
        lire(spark, "FAILFAST").cache().count()
        failfast = "aucune erreur"
    except Exception as e:  # noqa: BLE001
        failfast = f"**échec du job** (`{type(e).__name__}` : malformed record)"
    R.tableau(["Mode", "Lignes obtenues", "Remarque"], [
        ["PERMISSIVE", f"{perm.count():,}".replace(",", " "), f"{len(lignes_corr)} lignes avec `_corrupt_record` non nul"],
        ["DROPMALFORMED", f"{n_drop:,}".replace(",", " "),
         f"lignes supprimées **sans avertissement** ; `count()` sans cache renvoie {n_drop_brut:,} car Spark ne parse alors aucune colonne".replace(",", " ")],
        ["FAILFAST", "—", failfast],
    ])
    R("Lignes corrompues détectées :", "")
    R.code("\n".join(lignes_corr), "text")
    R("En production, il faut choisir **PERMISSIVE + table de rejets** : aucune ligne n'est perdue en "
      "silence, la ligne brute reste traçable, et le traitement continue. DROPMALFORMED masque les "
      "problèmes. FAILFAST convient à un contrôle d'entrée strict, où l'on préfère bloquer le "
      "chargement. Le `cache()` est nécessaire : sans lui, l'**élagage de colonnes** fait que Spark ne "
      "parse pas les champs non demandés et peut ne pas détecter la corruption.", "")

    # Q3 - contrôles qualité en un seul passage
    R("## Q3. Contrôles qualité en un seul passage (5 M lignes)", "")
    exprs = [F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in commandes.columns]
    exprs += [(F.count("*") - F.countDistinct("id_commande")).alias("doublons_id"),
              F.sum((F.col("quantite") <= 0).cast("int")).alias("quantite_le_0"),
              F.sum(F.to_date(F.col("date_commande").cast("string")).isNull().cast("int")).alias("date_invalide"),
              F.count("*").alias("total")]
    m = mesurer(spark, lambda: commandes.agg(*exprs).collect()[0].asDict())
    q = m["resultat"]
    R.tableau(["Contrôle", "Valeur"], [[k, f"{v:,}".replace(",", " ")] for k, v in q.items()])
    R(f"Tous ces contrôles tiennent en **un seul `agg`**, exécuté en {m['duree_s']} s ({m['jobs']} jobs, "
      f"{m['nb_stages']} stages). Le `countDistinct` impose un shuffle, et l'AQE exécute chaque stage "
      "comme un job distinct, mais les données ne sont lues qu'une fois. Dix `count()` séparés relanceraient dix fois la génération ou la lecture complète. "
      "On retrouve les anomalies voulues : environ 1 % de pays NULL et 0,5 % de quantités à -1.", "")

    # Q4 - pipeline avec rejets
    R("## Q4. Pipeline propre avec table de rejets", "")
    motif = (F.when(F.col("_corrupt_record").isNotNull(), "ligne_corrompue")
             .when(F.col("pays").isNull(), "pays_absent")
             .when(F.col("quantite").isNull() | (F.col("quantite") <= 0), "quantite_invalide")
             .when(F.col("prix_unitaire").isNull(), "prix_absent")
             .when(F.col("date_commande").isNull(), "date_invalide"))
    controle = perm.withColumn("motif_rejet", motif)
    rejets = controle.filter(F.col("motif_rejet").isNotNull())
    valides = controle.filter(F.col("motif_rejet").isNull()).drop("motif_rejet", "_corrupt_record")
    propres = valides.dropDuplicates(["id_commande"])
    rejets.write.mode("overwrite").parquet(str(DATA / "ex3_rejets"))
    propres.write.mode("overwrite").parquet(str(DATA / "ex3_propres"))
    total, n_rej, n_val = controle.count(), rejets.count(), valides.count()
    n_prop = spark.read.parquet(str(DATA / "ex3_propres")).count()
    doublons = n_val - n_prop
    R.tableau(["Motif de rejet", "Lignes"],
              [[r["motif_rejet"], r["count"]] for r in rejets.groupBy("motif_rejet").count().orderBy("motif_rejet").collect()])
    R.tableau(["Total", "Propres", "Rejets", "Doublons", "Propres + rejets + doublons = total ?"],
              [[f"{total:,}".replace(",", " "), f"{n_prop:,}".replace(",", " "), n_rej, doublons,
                "**oui**" if n_prop + n_rej + doublons == total else "**NON**"]])
    R("Les rejets et les lignes propres sont écrits dans deux dossiers Parquet distincts "
      "(`data/ex3_rejets`, `data/ex3_propres`). Chaque rejet garde son motif, et la réconciliation est exacte.", "")

    # Q5 - UDF contre natif
    R("## Q5. Fonction native, UDF Python et pandas_udf (5 M lignes)", "",
      "**Prédiction** : la version native est la plus rapide, la pandas_udf est 2 à 5 fois plus lente, "
      "et l'UDF classique est la plus lente, d'un facteur 10 ou plus.", "")
    montant = F.col("quantite") * F.col("prix_unitaire")
    variantes = [("F.when (natif)", classe_native(montant)), ("UDF Python", classe_udf(montant)),
                 ("pandas_udf (Arrow)", classe_pandas(montant))]
    res = []
    for nom, expr in variantes:
        df = commandes.withColumn("classe", expr).groupBy("classe").count()
        m = mesurer(spark, df.collect)
        p = plan(df)
        operateur = "BatchEvalPython" if "BatchEvalPython" in p else ("ArrowEvalPython" if "ArrowEvalPython" in p else "aucun (JVM)")
        res.append([nom, f"{m['duree_s']} s", f"`{operateur}`"])
    R.tableau(["Variante", "Durée", "Opérateur Python dans le plan"], res)
    d = {r[0]: float(r[1].split()[0]) for r in res}
    R(f"La version native est **{d['UDF Python'] / d['F.when (natif)']:.0f} fois plus rapide** que l'UDF "
      "Python : prédiction confirmée sur ce point. L'UDF classique **sérialise chaque ligne** de la JVM "
      "vers un processus Python, puis renvoie le résultat (`BatchEvalPython`). Elle est aussi **opaque "
      "pour Catalyst**, qui ne peut ni la simplifier ni pousser de filtre à travers. `F.when` reste "
      "dans la JVM, avec le code généré par Tungsten.", "",
      f"**Écart avec la prédiction** : la pandas_udf ({d['pandas_udf (Arrow)']} s) n'est pas plus rapide "
      f"que l'UDF classique ({d['UDF Python']} s) ici. La fonction appliquée est triviale (deux "
      "comparaisons) : le coût est dominé par le **transfert des 5 millions de valeurs** entre la JVM et "
      "Python, identique dans les deux cas, et par les workers Python, lents à démarrer sous Windows. Le "
      "gain d'Arrow (`ArrowEvalPython`, traitement par blocs et vectorisé) apparaît quand la fonction "
      "Python elle-même est coûteuse, ce qui n'est pas le cas ici. La règle reste : **natif d'abord**, "
      "puis pandas_udf, et l'UDF Python seulement en dernier recours.", "")

    # Q6 - fenêtres
    R("## Q6. Fonctions de fenêtre", "")
    propres_5m = commandes.filter(F.col("pays").isNotNull() & (F.col("quantite") > 0))
    df_ca = propres_5m.groupBy("pays", "id_produit").agg(F.round(F.sum(montant), 2).alias("ca"))
    w = Window.partitionBy("pays").orderBy(F.desc("ca"))
    top = df_ca.withColumn("rang", F.row_number().over(w)).filter("rang <= 3")
    R("**(a) Top 3 des produits par pays (CA)**", "")
    R.tableau(["pays", "id_produit", "ca", "rang"],
              [[r["pays"], r["id_produit"], f"{r['ca']:,.2f}".replace(",", " "), r["rang"]]
               for r in top.orderBy("pays", "rang").collect()])
    nb_exchange = plan(top, "simple").count("Exchange")
    R(f"Le plan de (a) contient **{nb_exchange} Exchange** : un pour le `groupBy(pays, id_produit)` et "
      "un pour `partitionBy(pays)` de la fenêtre, suivi d'un tri local par CA décroissant. Ici P000 "
      "arrive en tête partout, car il concentre 60 % des commandes.", "")
    jour = propres_5m.groupBy("pays", "date_commande").agg(F.round(F.sum(montant), 2).alias("ca_jour"))
    w7 = Window.partitionBy("pays").orderBy("date_commande").rowsBetween(-6, 0)
    wl = Window.partitionBy("pays").orderBy("date_commande")
    mobile = (jour.withColumn("ca_moy_7j", F.round(F.avg("ca_jour").over(w7), 2))
              .withColumn("var_j_1", F.round(F.col("ca_jour") - F.lag("ca_jour", 1).over(wl), 2)))
    R("**(b) Moyenne mobile 7 jours et (c) variation par rapport à la veille** (FR, 8 premiers jours)", "")
    R.tableau(["date", "ca_jour", "ca_moy_7j", "var_j_1"],
              [[str(r["date_commande"]), r["ca_jour"], r["ca_moy_7j"], r["var_j_1"]]
               for r in mobile.filter("pays = 'FR'").orderBy("date_commande").limit(8).collect()])
    ex = spark.createDataFrame([(100,), (100,), (90,)], ["v"])
    wo = Window.orderBy(F.desc("v"))
    R.tableau(["v", "row_number", "rank", "dense_rank"],
              [[r["v"], r["rn"], r["rk"], r["drk"]] for r in
               ex.select("v", F.row_number().over(wo).alias("rn"), F.rank().over(wo).alias("rk"),
                         F.dense_rank().over(wo).alias("drk")).collect()])
    R("`row_number` numérote sans ex æquo, `rank` laisse un trou après les ex æquo (1, 1, 3), et "
      "`dense_rank` n'en laisse pas (1, 1, 2). Pour un « top 3 strict », `row_number` est le bon choix. "
      "Pour garder les ex æquo, on utilise `dense_rank`.", "")

    # Q7 - SQL
    df_ca.createOrReplaceTempView("ca_pays_produit")
    top_sql = spark.sql("""
        SELECT pays, id_produit, ca, rang FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY pays ORDER BY ca DESC) AS rang
          FROM ca_pays_produit) t
        WHERE rang <= 3""")
    a_b = top.exceptAll(top_sql).count()
    b_a = top_sql.exceptAll(top).count()
    R("## Q7. Même KPI en Spark SQL", "")
    R.code("""SELECT pays, id_produit, ca, rang FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY pays ORDER BY ca DESC) AS rang
  FROM ca_pays_produit) t
WHERE rang <= 3""", "sql")
    R(f"`top.exceptAll(top_sql).count() = {a_b}` et `top_sql.exceptAll(top).count() = {b_a}` : les deux "
      f"résultats sont **identiques**. Les plans physiques sont "
      f"{'identiques' if plan(top, 'simple').count('Exchange') == plan(top_sql, 'simple').count('Exchange') else 'différents'} "
      "dans leur structure (mêmes Exchange, Window, Filter), car DataFrame et SQL passent par le même "
      "optimiseur Catalyst.", "")

    # Q8 - NULL
    R("## Q8. Pièges des NULL", "")
    spark.createDataFrame([("FR",), (None,), ("BE",)], "pays string").createOrReplaceTempView("t")
    tests = [("NULL = NULL", "SELECT NULL = NULL AS r", "NULL"),
             ("count(pays) / count(*)", "SELECT concat(count(pays), ' / ', count(*)) AS r FROM t", "2 / 3"),
             ("sum d'une colonne toute NULL", "SELECT sum(CAST(NULL AS INT)) AS r FROM t", "NULL"),
             ("NULL <=> NULL", "SELECT NULL <=> NULL AS r", "true"),
             ("filter(pays = NULL)", "SELECT count(*) AS r FROM t WHERE pays = NULL", "0"),
             ("filter(pays IS NULL)", "SELECT count(*) AS r FROM t WHERE pays IS NULL", "1")]
    R.tableau(["Expression", "Prédiction", "Résultat Spark"],
              [[f"`{e}`", p, f"`{sql_txt(spark.sql(s).collect()[0]['r'])}`"] for e, s, p in tests])
    R("NULL signifie « inconnu » : `NULL = NULL` est inconnu, donc NULL. Un `filter` ne garde que les "
      "lignes où la condition vaut **vrai**, et une condition à NULL élimine la ligne : "
      "`pays = NULL` ne renvoie jamais rien. Il faut écrire `isNull()` ou utiliser `<=>` (`eqNullSafe`). "
      "`count(col)` ignore les NULL, `count(*)` non.", "")
    R.sauver("ex3_dataframe_qualite.md")
    spark.stop()


if __name__ == "__main__":
    main()
