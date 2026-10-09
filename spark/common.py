"""
Socle commun du TP Spark : session, jeu de données et mesures lues dans la Spark UI.

Les mesures (jobs, stages, tâches, shuffle, durée des tâches) sont lues via l'API REST
de la Spark UI (http://localhost:4040/api/v1), c'est-à-dire exactement ce que montrent
les onglets Jobs et Stages.
"""
from __future__ import annotations

import json
import os
import statistics
import sys
import time
import urllib.request
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession, functions as F  # noqa: E402

RACINE = Path(__file__).resolve().parents[1]
DATA = RACINE / "spark" / "data"
RESULTATS = RACINE / "spark" / "resultats"
SHIM = RACINE / "tools" / "winfs-shim" / "winfs-shim.jar"
N = int(os.environ.get("TP_N", 5_000_000))


def session(master: str = "local[4]", **conf) -> SparkSession:
    b = (SparkSession.builder.master(master).appName("TP-Spark")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", "6g")
         .config("spark.ui.showConsoleProgress", "false"))
    if os.name == "nt":  # Windows sans winutils.exe : voir tools/winfs-shim
        b = (b.config("spark.driver.extraClassPath", str(SHIM))
              .config("spark.hadoop.fs.file.impl", "fr.tague.winfs.NoPermLocalFileSystem")
              .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "fr.tague.winfs.NoPermLocalFs"))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def commandes_et_produits(spark: SparkSession, n: int = N):
    """Jeu de données de l'énoncé : 60 % des commandes sur P000, 1 % de pays absents, 0,5 % de quantités -1."""
    pays = F.array(*[F.lit(p) for p in ["FR", "BE", "ES", "DE", "IT"]])
    commandes = (spark.range(n).withColumnRenamed("id", "n")
                 .withColumn("id_commande", F.concat(F.lit("C"), F.col("n").cast("string")))
                 .withColumn("id_produit", F.when(F.rand(1) < 0.6, F.lit("P000"))
                             .otherwise(F.concat(F.lit("P"), F.lpad((F.rand(2) * 999 + 1).cast("int").cast("string"), 3, "0"))))
                 .withColumn("pays", F.when(F.rand(3) >= 0.01, F.element_at(pays, (F.rand(4) * 5).cast("int") + 1)))
                 .withColumn("quantite", F.when(F.rand(5) < 0.005, F.lit(-1)).otherwise((F.rand(6) * 20).cast("int") + 1))
                 .withColumn("prix_unitaire", F.round(F.rand(7) * 100 + 1, 2))
                 .withColumn("date_commande", F.date_add(F.lit("2026-01-01").cast("date"), (F.rand(8) * 270).cast("int")))
                 .drop("n"))
    familles = F.array(*[F.lit(f) for f in ["Eclairage", "Protection", "Cablage", "Domotique"]])
    produits = (spark.range(1000)
                .withColumn("id_produit", F.concat(F.lit("P"), F.lpad(F.col("id").cast("string"), 3, "0")))
                .withColumn("famille", F.element_at(familles, (F.col("id") % 4).cast("int") + 1))
                .drop("id"))
    return commandes, produits


# ------------------------------------------------------------------ Spark UI
def _api(spark: SparkSession, chemin: str):
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{chemin}"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def _jobs(spark):
    return {j["jobId"]: j for j in _api(spark, "jobs")}


def mesurer(spark: SparkSession, action, details_taches: bool = False) -> dict:
    """Exécute action() et renvoie la durée et ce que la Spark UI a enregistré pour cette action."""
    avant = set(_jobs(spark))
    t0 = time.perf_counter()
    resultat = action()
    duree = time.perf_counter() - t0
    for _ in range(50):                       # l'UI est alimentée de façon asynchrone
        nouveaux = {k: v for k, v in _jobs(spark).items() if k not in avant}
        if all(j["status"] != "RUNNING" for j in nouveaux.values()):
            break
        time.sleep(0.1)
    time.sleep(0.3)
    nouveaux = {k: v for k, v in _jobs(spark).items() if k not in avant}
    stage_ids = sorted({s for j in nouveaux.values() for s in j["stageIds"]})
    stages = []
    for sid in stage_ids:
        for st in _api(spark, f"stages/{sid}"):
            if st["status"] == "SKIPPED":
                continue
            info = {"stage": sid, "taches": st["numTasks"], "echecs": st.get("numFailedTasks", 0),
                    "shuffle_write": st.get("shuffleWriteBytes", 0), "shuffle_read": st.get("shuffleReadBytes", 0),
                    "entree": st.get("inputBytes", 0), "nom": st["name"][:60]}
            if details_taches:
                taches = _api(spark, f"stages/{sid}/{st['attemptId']}/taskList?length=10000")
                d = [t["taskMetrics"]["executorRunTime"] for t in taches if t.get("taskMetrics")]
                if d:
                    info["tache_max_ms"] = max(d)
                    info["tache_mediane_ms"] = statistics.median(d)
            stages.append(info)
    return {"resultat": resultat, "duree_s": round(duree, 2), "jobs": len(nouveaux),
            "stages": stages, "nb_stages": len(stages),
            "shuffle_write": sum(s["shuffle_write"] for s in stages),
            "shuffle_read": sum(s["shuffle_read"] for s in stages)}


def octets(n: float) -> str:
    for unite in ["o", "Ko", "Mo", "Go"]:
        if abs(n) < 1024:
            return f"{n:.1f} {unite}" if unite != "o" else f"{int(n)} o"
        n /= 1024
    return f"{n:.1f} To"


def taille_dossier(chemin: Path) -> int:
    return sum(f.stat().st_size for f in Path(chemin).rglob("*") if f.is_file() and not f.name.endswith(".crc"))


class Rapport:
    """Accumule le compte rendu Markdown d'un exercice."""

    def __init__(self, titre: str):
        n = f"{N:,}".replace(",", " ")
        self.lignes = [f"# {titre}", "",
                       f"_Résultats générés par l'exécution réelle du script (PySpark {self._version()}, "
                       f"local, N = {n} commandes). Les durées dépendent de la machine._", ""]

    @staticmethod
    def _version():
        import pyspark
        return pyspark.__version__

    def __call__(self, *lignes: str):
        self.lignes.extend(lignes)

    def tableau(self, entetes: list[str], lignes: list[list]):
        self.lignes.append("| " + " | ".join(entetes) + " |")
        self.lignes.append("|" + "|".join("---" for _ in entetes) + "|")
        for l in lignes:
            self.lignes.append("| " + " | ".join(str(x) for x in l) + " |")
        self.lignes.append("")

    def code(self, texte: str, langage: str = ""):
        self.lignes += [f"```{langage}", texte.rstrip(), "```", ""]

    def sauver(self, nom: str):
        RESULTATS.mkdir(parents=True, exist_ok=True)
        texte = "\n".join(self.lignes) + "\n"
        for chemin in sorted({RACINE.as_uri(), str(RACINE), RACINE.as_posix()}, key=len, reverse=True):
            texte = texte.replace(chemin, ".")          # pas de chemin local dans les comptes rendus
        (RESULTATS / nom).write_text(texte, encoding="utf-8")
        print(f"-> {RESULTATS / nom}")


def charger(spark: SparkSession):
    """Commandes et produits matérialisés une fois en Parquet (data/), relus ensuite."""
    chemin_c, chemin_p = DATA / "commandes_parquet", DATA / "produits_parquet"
    if not (chemin_c / "_SUCCESS").exists():
        commandes, produits = commandes_et_produits(spark)
        commandes.write.mode("overwrite").parquet(str(chemin_c))
        produits.write.mode("overwrite").parquet(str(chemin_p))
    return spark.read.parquet(str(chemin_c)), spark.read.parquet(str(chemin_p))


def stage_le_plus_lent(m: dict) -> dict:
    """Stage de la mesure dont la tâche la plus longue est la plus longue (le stage de jointure en cas de skew)."""
    candidats = [s for s in m["stages"] if "tache_max_ms" in s]
    return max(candidats, key=lambda s: s["tache_max_ms"]) if candidats else {}
