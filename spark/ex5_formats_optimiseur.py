"""Exercice 5 - Formats, stockage, cache et optimiseur Catalyst."""
import contextlib
import io
import re
import shutil
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from common import DATA, Rapport, charger, mesurer, octets, session, taille_dossier

SCHEMA = StructType([StructField("id_commande", StringType()), StructField("id_produit", StringType()),
                     StructField("pays", StringType()), StructField("quantite", IntegerType()),
                     StructField("prix_unitaire", DoubleType()), StructField("date_commande", DateType())])


def plan(df, mode="formatted") -> str:
    t = io.StringIO()
    with contextlib.redirect_stdout(t):
        df.explain(mode)
    return t.getvalue()


def lu(m) -> int:
    return sum(s["entree"] for s in m["stages"])


def octets_colonnes(dossier, colonnes) -> int:
    """Octets compressés des colonnes lues, d'après les métadonnées des fichiers Parquet (pied de fichier)."""
    import pyarrow.parquet as pq
    total = 0
    for f in Path(dossier).rglob("*.parquet"):
        meta = pq.ParquetFile(f).metadata
        for rg in range(meta.num_row_groups):
            groupe = meta.row_group(rg)
            for c in range(groupe.num_columns):
                if groupe.column(c).path_in_schema in colonnes:
                    total += groupe.column(c).total_compressed_size
    return total


def ligne_plan(p, cle):
    m = re.search(rf"{cle}: (.*)", p)
    return m.group(1).strip() if m else "—"


@udf(DoubleType())
def montant_udf(q, p):
    return float(q) * float(p) if q is not None and p is not None else None


def main():
    spark = session()
    commandes, produits = charger(spark)
    propres = commandes.filter(F.col("pays").isNotNull() & (F.col("quantite") > 0))
    R = Rapport("Exercice 5 — Formats, stockage, cache et optimiseur")
    chemins = {f: DATA / f"ex5_{f}" for f in ["csv", "json", "parquet"]}

    # Q1 - formats
    R("## Q1. CSV, JSON et Parquet (snappy)", "",
      "**Prédiction** : le Parquet sera 3 à 5 fois plus petit que le CSV et le plus rapide à relire. Le "
      "JSON sera le plus volumineux, car il répète le nom des colonnes sur chaque ligne.", "")
    res = []
    for fmt, chemin in chemins.items():
        ecr = propres.write.mode("overwrite")
        t0 = time.perf_counter()
        {"csv": lambda: ecr.option("header", True).csv(str(chemin)),
         "json": lambda: ecr.json(str(chemin)),
         "parquet": lambda: ecr.option("compression", "snappy").parquet(str(chemin))}[fmt]()
        t_ecr = time.perf_counter() - t0
        lecteur = {"csv": lambda: spark.read.schema(SCHEMA).option("header", True).csv(str(chemin)),
                   "json": lambda: spark.read.schema(SCHEMA).json(str(chemin)),
                   "parquet": lambda: spark.read.parquet(str(chemin))}[fmt]
        mesurer(spark, lambda: lecteur().agg(F.avg("prix_unitaire")).collect())     # préchauffage
        m = mesurer(spark, lambda: lecteur().agg(F.avg("prix_unitaire")).collect())
        volume = octets_colonnes(chemin, {"prix_unitaire"}) if fmt == "parquet" else lu(m)
        res.append([fmt.upper(), octets(taille_dossier(chemin)), f"{t_ecr:.1f} s", f"{m['duree_s']} s", octets(volume)])
    R.tableau(["Format", "Taille disque", "Écriture", "Lecture + avg(prix)", "Volume à lire"], res)
    R("_Volume à lire : pour CSV et JSON, octets lus mesurés par Spark (le fichier entier) ; pour Parquet, "
      "taille compressée de la seule colonne `prix_unitaire`, d'après les métadonnées des fichiers._", "")
    R("Parquet est **colonnaire** : pour `avg(prix_unitaire)`, Spark ne lit que la colonne demandée "
      "(quelques Mo au lieu du fichier entier). Il est aussi **compressé** (snappy, plus un encodage par "
      "dictionnaire très efficace sur `pays` ou `id_produit`) et **typé** : le schéma est embarqué et "
      "il n'y a aucun texte à parser. Le CSV doit lire et découper chaque ligne entière. Le JSON répète "
      "en plus les noms de colonnes sur chaque ligne, c'est le plus lourd.", "")

    # Q2 - pushdown
    R("## Q2. Élagage de colonnes et predicate pushdown", "")
    pq = spark.read.parquet(str(chemins["parquet"])).select("pays", "quantite").filter(F.col("pays") == "FR")
    cs = (spark.read.schema(SCHEMA).option("header", True).csv(str(chemins["csv"]))
          .select("pays", "quantite").filter(F.col("pays") == "FR"))
    m_pq, m_cs = mesurer(spark, pq.count), mesurer(spark, cs.count)
    p_pq, p_cs = plan(pq), plan(cs)
    R.tableau(["Source", "ReadSchema", "PushedFilters", "Volume lu"], [
        ["Parquet", f"`{ligne_plan(p_pq, 'ReadSchema')}`", f"`{ligne_plan(p_pq, 'PushedFilters')}`",
         octets(octets_colonnes(chemins["parquet"], {"pays", "quantite"}))],
        ["CSV", f"`{ligne_plan(p_cs, 'ReadSchema')}`", f"`{ligne_plan(p_cs, 'PushedFilters')}`", octets(lu(m_cs))]])
    R("Dans les deux cas, Catalyst réduit le `ReadSchema` à 2 colonnes et pousse le filtre vers la "
      "source. **Mais l'effet n'est pas le même.** Le lecteur Parquet ne lit physiquement que les "
      "colonnes `pays` et `quantite`, et utilise les statistiques min/max de chaque *row group* pour "
      "sauter les blocs sans « FR ». Le lecteur CSV doit quand même lire **tout le fichier texte** "
      "octet par octet : il ne parse que les colonnes utiles, mais le volume lu reste le fichier entier.", "")

    # Q3 - partitionBy
    R("## Q3. `partitionBy` et petits fichiers", "")
    par_pays = DATA / "ex5_par_pays"
    propres.repartition("pays").write.mode("overwrite").partitionBy("pays").parquet(str(par_pays))
    fr = spark.read.parquet(str(par_pays)).filter(F.col("pays") == "FR")
    fr.count()
    R(f"`partitionBy('pays')` crée **{len([d for d in par_pays.iterdir() if d.is_dir()])} dossiers** "
      f"(`pays=FR`, `pays=BE`…). Plan de la requête `pays = 'FR'` : "
      f"`PartitionFilters: {ligne_plan(plan(fr), 'PartitionFilters')}`. Fichiers ouverts : "
      f"{octets(taille_dossier(par_pays / 'pays=FR'))} (dossier `pays=FR`), contre "
      f"{octets(taille_dossier(par_pays))} pour tout le jeu. Seul le dossier `pays=FR` est ouvert : "
      "c'est le **partition pruning**.", "")
    res = []
    for nom, df in [("`partitionBy('id_produit')` direct", propres.repartition(8)),
                    ("`repartition('id_produit')` puis `partitionBy`", propres.repartition("id_produit"))]:
        cible = DATA / "ex5_par_produit"
        shutil.rmtree(cible, ignore_errors=True)
        t0 = time.perf_counter()
        df.write.mode("overwrite").partitionBy("id_produit").parquet(str(cible))
        duree = time.perf_counter() - t0
        fichiers = [f for f in cible.rglob("*.parquet")]
        dossiers = [d for d in cible.iterdir() if d.is_dir()]
        res.append([nom, len(dossiers), len(fichiers), octets(taille_dossier(cible) / max(len(fichiers), 1)), f"{duree:.1f} s"])
    R.tableau(["Écriture", "Dossiers", "Fichiers Parquet", "Taille moyenne d'un fichier", "Durée"], res)
    R("Partitionner sur une colonne à **1 000 valeurs** crée 1 000 dossiers. Sans précaution, chacune "
      "des 8 tâches d'écriture produit un fichier dans chaque dossier où elle a des lignes, soit "
      "jusqu'à 8 000 petits fichiers de quelques Ko. Chaque fichier coûte un objet de métadonnées (sur "
      "HDFS, dans la mémoire du NameNode), une ouverture et un listing à chaque lecture. Un "
      "`repartition('id_produit')` avant l'écriture regroupe chaque produit dans une seule tâche : on "
      "obtient **un fichier par dossier**. `maxRecordsPerFile` sert au cas inverse, pour plafonner la "
      "taille des gros fichiers. Ici `pays` (5 valeurs) est une bonne clé de partition, mais "
      "`id_produit` (1 000 valeurs) ne l'est pas.", "")

    # Q4 - cache
    R("## Q4. Cache et persistance", "")

    def pipeline():
        return (spark.read.parquet(str(chemins["parquet"]))
                .withColumn("montant", F.col("quantite") * F.col("prix_unitaire"))
                .join(produits, "id_produit")
                .groupBy("pays", "famille", "date_commande").agg(F.sum("montant").alias("ca"), F.count("*").alias("n")))

    def trois_kpi(df):
        df.groupBy("pays").agg(F.sum("ca")).collect()
        df.groupBy("famille").agg(F.sum("ca")).collect()
        df.agg(F.max("ca"), F.sum("n")).collect()

    res = []
    for nom, preparer in [("sans cache", lambda d: d), ("cache()", lambda d: d.cache()),
                          ("persist(MEMORY_AND_DISK)", lambda d: d.persist(StorageLevel.MEMORY_AND_DISK))]:
        df = preparer(pipeline())
        t0 = time.perf_counter()
        trois_kpi(df)
        res.append([nom, f"{time.perf_counter() - t0:.2f} s"])
        df.unpersist()
    R.tableau(["Variante", "Durée des 3 KPI"], res)
    R("Sans cache, chacun des 3 KPI relit le Parquet, refait la jointure et l'agrégation. Avec "
      "`cache()`, le premier KPI matérialise le résultat intermédiaire, qui apparaît dans l'onglet "
      "Storage, et les deux suivants le relisent. `unpersist()` libère la mémoire ensuite.", "",
      "**Cas où le cache ne sert à rien** : un DataFrame utilisé une seule fois. On paie alors le "
      "stockage sans jamais le relire, et la mémoire prise peut forcer des déversements ailleurs. Autre "
      "cas : un `cache()` placé avant un filtre très sélectif, qui stocke des données que l'on va jeter.", "",
      "**cache() ou checkpoint()** : le cache garde le **lignage**. Si une partition est perdue, Spark la "
      "recalcule. `checkpoint()` écrit le résultat sur disque, dans `setCheckpointDir`, et **coupe le "
      "lignage**. C'est utile pour des plans très longs, par exemple des itérations ou des jointures "
      "successives, dont l'analyse et la reprise deviendraient coûteuses.", "")
    spark.sparkContext.setCheckpointDir(str(DATA / "ckpt"))
    petit = spark.range(10).withColumn("x", F.col("id") * 2)
    ck = petit.checkpoint()
    R.code(f"Avant checkpoint : {plan(petit, 'simple').strip().splitlines()[-1].strip()}\n"
           f"Après checkpoint : {plan(ck, 'simple').strip().splitlines()[-1].strip()}", "text")

    # Q5 - Catalyst
    R("## Q5. Lire Catalyst", "")
    cmd = spark.read.parquet(str(chemins["parquet"]))
    apres = cmd.join(produits, "id_produit").filter(F.col("pays") == "FR").select("famille", "quantite")
    avant = cmd.filter(F.col("pays") == "FR").join(produits, "id_produit").select("famille", "quantite")
    ext = plan(apres, "extended")
    sections = re.findall(r"== (.*?) ==", ext)
    opt_a = apres._jdf.queryExecution().optimizedPlan().toString()
    opt_b = avant._jdf.queryExecution().optimizedPlan().toString()
    norm = lambda s: re.sub(r"#\d+L?", "", s)  # noqa: E731
    R(f"`explain('extended')` affiche les plans : {', '.join(f'**{s}**' for s in sections)}.", "")
    R("Plan **optimisé** de la version « filtre après la jointure » :", "")
    R.code(opt_a, "text")
    R(f"Les plans optimisés des deux écritures sont **{'identiques' if norm(opt_a) == norm(opt_b) else 'différents'}** "
      "(une fois les identifiants internes `#123` retirés). Catalyst a **poussé le filtre** `pays = FR` "
      "sous la jointure, jusqu'au scan Parquet. Il a **élagué les colonnes**, en ne lisant que "
      "`id_produit`, `pays` et `quantite`. Il a aussi ajouté `isnotnull(id_produit)`, déduit de la "
      "jointure interne.", "")
    const = spark.range(3).select((F.col("id") + F.lit(2) * F.lit(3)).alias("v"))
    R("**Repli de constantes** : `id + lit(2) * lit(3)` devient, dans le plan optimisé :", "")
    R.code(const._jdf.queryExecution().optimizedPlan().toString(), "text")

    # Q6 - benchmark
    R("## Q6. Mini-benchmark : CA par pays et famille", "")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    naif = (spark.read.option("header", True).option("inferSchema", True).csv(str(chemins["csv"]))
            .join(produits, "id_produit")
            .withColumn("montant", montant_udf("quantite", "prix_unitaire"))
            .groupBy("pays", "famille").agg(F.sum("montant").alias("ca")))
    m_naif = mesurer(spark, naif.collect)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    opti = (spark.read.parquet(str(chemins["parquet"])).select("id_produit", "pays", "quantite", "prix_unitaire")
            .join(F.broadcast(produits), "id_produit")
            .groupBy("pays", "famille").agg(F.sum(F.col("quantite") * F.col("prix_unitaire")).alias("ca")))
    m_opti = mesurer(spark, opti.collect)
    ecart = max(abs(a["ca"] - b["ca"]) for a, b in zip(sorted(m_naif["resultat"]), sorted(m_opti["resultat"])))
    R.tableau(["Version", "Ingrédients", "Durée", "Jobs", "Shuffle write"], [
        ["Naïve", "CSV + inferSchema, UDF Python, SortMergeJoin subie", f"{m_naif['duree_s']} s", m_naif["jobs"], octets(m_naif["shuffle_write"])],
        ["Optimisée", "Parquet, fonctions natives, broadcast, colonnes utiles", f"{m_opti['duree_s']} s", m_opti["jobs"], octets(m_opti["shuffle_write"])]])
    R(f"**Gain : × {m_naif['duree_s'] / m_opti['duree_s']:.0f}**, pour le même résultat (écart maximal "
      f"entre les deux CA : {ecart:.6f}).", "",
      "Classement des optimisations par impact, dans l'ordre observé dans les exercices 3 à 5 :",
      "1. **Format et schéma** : passer du CSV avec inférence au Parquet supprime une passe de lecture et le parsing, et divise le volume lu.",
      "2. **Supprimer l'UDF Python** : c'est le facteur le plus lourd de l'exercice 3, car il évite la sérialisation de chaque ligne entre la JVM et Python.",
      "3. **Broadcast** : il supprime le shuffle des 5 M faits et le skew sur P000 (exercice 4).",
      "4. **Filtres et colonnes tôt** : Catalyst le fait souvent seul, mais il ne peut pas le faire à travers une UDF.",
      "5. **Cache** : utile seulement quand un résultat est réutilisé.", "")
    R.sauver("ex5_formats_optimiseur.md")
    spark.stop()


if __name__ == "__main__":
    main()
