"""
TP 6 Hadoop - Mission E : traitement Spark des ventes (le « pseudo-code » de l'énoncé, exécuté pour de vrai).

Lit les ventes, écarte les doublons, les quantités ≤ 0 et les produits au prix de référence invalide,
puis agrège le chiffre d'affaires par pays. Sur un cluster, les chemins seraient hdfs:///data/...
"""
import sys
from pathlib import Path

ICI = Path(__file__).resolve().parent
sys.path.insert(0, str(ICI.parents[1] / "spark"))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType  # noqa: E402

from common import session  # noqa: E402

DATA = ICI.parents[0] / "tp2_admin_hdfs" / "data"
SCHEMA_VENTES = StructType([StructField("date", DateType()), StructField("pays", StringType()),
                            StructField("produit", StringType()), StructField("quantite", IntegerType()),
                            StructField("prix", DoubleType())])


def main():
    spark = session("local[2]")
    ventes = spark.read.schema(SCHEMA_VENTES).option("header", True).csv(str(DATA / "ventes.csv"))
    produits = spark.read.option("header", True).option("inferSchema", True).csv(str(DATA / "produits.csv"))

    brut = ventes.count()
    dedup = ventes.dropDuplicates()
    valides = dedup.filter((F.col("quantite") > 0) & F.col("pays").isNotNull())
    produits_ok = produits.filter(F.col("prix_ref") > 0)                       # P007 (-24 €) écarté
    enrichies = valides.join(F.broadcast(produits_ok), valides.produit == produits_ok.produit_id, "left_semi")
    ca = (enrichies.groupBy("pays")
          .agg(F.round(F.sum(F.col("quantite") * F.col("prix")), 2).alias("ca"), F.count("*").alias("ventes"))
          .orderBy(F.desc("ca")))

    lignes = ["# TP 6 — Mission E : CA par pays calculé avec Spark", "",
              "_Sortie réelle de `spark_ca_par_pays.py` sur `tp2_admin_hdfs/data/ventes.csv`._", "",
              f"- Lignes lues : **{brut}**",
              f"- Après suppression du doublon exact : **{dedup.count()}**",
              f"- Ventes écartées car le produit a un prix de référence invalide (P007) : "
              f"**{valides.count() - enrichies.count()}**", "",
              "| Pays | CA (€) | Ventes |", "|---|---:|---:|"]
    lignes += [f"| {r['pays']} | {r['ca']:,.2f} | {r['ventes']} |".replace(",", " ") for r in ca.collect()]
    total = ca.agg(F.sum("ca")).first()[0]
    lignes += [f"| **Total** | **{total:,.2f}** | |".replace(",", " "), ""]
    (ICI / "resultat_spark.md").write_text("\n".join(lignes) + "\n", encoding="utf-8")
    print("\n".join(lignes))
    spark.stop()


if __name__ == "__main__":
    main()
