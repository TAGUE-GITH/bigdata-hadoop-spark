"""Exercice 6 - Structured Streaming : micro-batchs, fenêtres, watermark, checkpoint, foreachBatch, jointure."""
import contextlib
import datetime as dt
import io
import json
import random
import shutil
import threading
import time

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from common import DATA, Rapport, session

IOT = DATA / "iot"
ENTREE = IOT / "incoming"
SCHEMA = StructType([StructField("event_id", StringType()), StructField("machine_id", StringType()),
                     StructField("temperature", DoubleType()), StructField("event_time", TimestampType())])
NB_FICHIERS, PAUSE = 15, 2          # énoncé : 40 fichiers toutes les 5 s ; réduit pour exécuter le TP en ~1 min
RETARDS = []                        # vérité terrain : retard de chaque événement généré


def generateur():
    """Générateur de l'énoncé : 20 événements par fichier, retards de 0, 30, 90 s ou 10 min."""
    random.seed(7)
    for b in range(NB_FICHIERS):
        now = dt.datetime.now()
        evts = []
        for i in range(20):
            retard = random.choice([0, 0, 0, 30, 90, 600])
            RETARDS.append(retard)
            t = now - dt.timedelta(seconds=retard)
            evts.append({"event_id": f"{b}-{i}", "machine_id": f"M{random.randint(1, 3)}",
                         "temperature": round(random.gauss(75, 8), 1), "event_time": t.strftime("%Y-%m-%dT%H:%M:%S")})
        tmp = ENTREE / f".b{b:03d}.tmp"
        tmp.write_text("\n".join(json.dumps(e) for e in evts))
        tmp.rename(ENTREE / f"b{b:03d}.json")     # dépôt atomique : jamais de fichier à moitié écrit
        time.sleep(PAUSE)


def plan_streaming(q) -> str:
    t = io.StringIO()
    with contextlib.redirect_stdout(t):
        q.explain()
    return t.getvalue()


def main():
    shutil.rmtree(IOT, ignore_errors=True)
    ENTREE.mkdir(parents=True)
    spark = session()
    R = Rapport("Exercice 6 — Structured Streaming")
    R(f"_Générateur de l'énoncé lancé dans un thread : {NB_FICHIERS} fichiers de 20 événements, un toutes "
      f"les {PAUSE} s (au lieu de 40 toutes les 5 s, pour limiter la durée d'exécution). Les retards de "
      "0 s, 30 s, 90 s et 10 min sont inchangés._", "")
    flux = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(ENTREE))
    fenetre = F.window("event_time", "1 minute")

    gen = threading.Thread(target=generateur)
    gen.start()

    # Q1 - premier flux
    q_brut = (flux.writeStream.format("memory").queryName("brut").outputMode("append")
              .trigger(processingTime="2 seconds").start())
    # Q2 - modes de sortie sans watermark
    agg = flux.groupBy(fenetre, "machine_id").agg(F.round(F.avg("temperature"), 1).alias("temp_moy"), F.count("*").alias("n"))
    q_complete = agg.writeStream.format("memory").queryName("agg_complete").outputMode("complete").start()
    q_update = agg.writeStream.format("memory").queryName("agg_update").outputMode("update").start()
    try:
        agg.writeStream.format("memory").queryName("agg_append").outputMode("append").start()
        refus_append = "accepté (inattendu)"
    except Exception as e:  # noqa: BLE001
        refus_append = str(e).split("\n")[0][:220]
    # Q3 - watermark
    agg_wm = (flux.withWatermark("event_time", "2 minutes").groupBy(fenetre, "machine_id")
              .agg(F.round(F.avg("temperature"), 1).alias("temp_moy"), F.count("*").alias("n")))
    q_wm = agg_wm.writeStream.format("memory").queryName("agg_wm").outputMode("update").start()
    q_wm_append = agg_wm.writeStream.format("memory").queryName("agg_wm_append").outputMode("append").start()

    gen.join()
    for q in (q_brut, q_complete, q_update, q_wm, q_wm_append):
        q.processAllAvailable()

    p = q_brut.lastProgress
    R("## Q1. Premier flux (sink mémoire, append, trigger 2 s)", "")
    R.code(json.dumps({k: p[k] for k in ["batchId", "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond"]},
                      indent=2), "json")
    R(f"Total reçu : **{spark.table('brut').count()} événements** sur {len(RETARDS)} générés. Le **schéma "
      "est obligatoire** pour une source de fichiers : Spark ne peut pas inférer le schéma d'un dossier "
      "qui grossit sans relire les données à chaque batch, et un flux doit garder un plan stable du "
      "début à la fin. `maxFilesPerTrigger = 1` limite chaque micro-batch à un seul fichier. Cela régule "
      "le débit et rend chaque batch observable, au lieu de tout absorber d'un coup au démarrage. Sur "
      "Colab ou sur un sink de test, le sink **memory** remplace la console.", "")

    R("## Q2. Fenêtres d'une minute et modes de sortie", "")
    nb_c = [len(x["sources"]) and x["numInputRows"] for x in q_complete.recentProgress]
    R.tableau(["Mode", "Lignes dans la table résultat", "Comportement"], [
        ["complete", spark.table("agg_complete").count(), "toute la table d'agrégats est réécrite à chaque batch"],
        ["update", spark.table("agg_update").count(), "seules les fenêtres modifiées sont émises ; une fenêtre peut apparaître plusieurs fois"],
        ["append (sans watermark)", "—", f"**refusé** : `{refus_append}`"]])
    R("Le mode **append** n'émet une ligne qu'une fois, quand elle est **définitive**. Sans watermark, "
      "une fenêtre n'est jamais définitive, car un événement en retard peut toujours la modifier. Spark "
      "refuse donc la requête dès `start()`. **complete** renvoie tout à chaque batch, ce qui est "
      "coûteux et suppose un état complet. **update** renvoie seulement ce qui a changé.", "")
    R("Extrait de la table `complete` :", "")
    R.tableau(["window", "machine", "temp_moy", "n"],
              [[f"{r['window'].start:%H:%M} → {r['window'].end:%H:%M}", r["machine_id"], r["temp_moy"], r["n"]]
               for r in spark.table("agg_complete").orderBy("window", "machine_id").limit(9).collect()])

    R("## Q3. Watermark de 2 minutes", "")
    etats_sans = [x["stateOperators"][0]["numRowsTotal"] for x in q_complete.recentProgress if x["stateOperators"]]
    etats_wm = [x["stateOperators"][0]["numRowsTotal"] for x in q_wm.recentProgress if x["stateOperators"]]
    ignores = sum(x["stateOperators"][0].get("numRowsDroppedByWatermark", 0) for x in q_wm.recentProgress if x["stateOperators"])
    comptes_wm = spark.sql("SELECT sum(n) s FROM (SELECT window, machine_id, max(n) n FROM agg_wm GROUP BY window, machine_id)").collect()[0]["s"]
    retard = {r: RETARDS.count(r) for r in sorted(set(RETARDS))}
    R.tableau(["Retard généré", "Événements"], [[f"{r} s", n] for r, n in retard.items()])
    comptes_sans = spark.sql("SELECT sum(n) s FROM agg_complete").collect()[0]["s"]
    R.tableau(["Indicateur", "Sans watermark", "Avec watermark 2 min"], [
        ["Événements comptés dans les fenêtres", comptes_sans, comptes_wm],
        ["Événements ignorés", len(RETARDS) - comptes_sans, len(RETARDS) - comptes_wm],
        ["Métrique `numRowsDroppedByWatermark` (lignes pré-agrégées)", 0, ignores],
        ["État : `numRowsTotal` (premiers → derniers batchs)",
         " → ".join(str(x) for x in etats_sans[:2] + etats_sans[-2:]),
         " → ".join(str(x) for x in etats_wm[:2] + etats_wm[-2:])],
        ["Fenêtres émises en append (définitives)", "refusé", spark.table("agg_wm_append").count()]])
    R(f"Le watermark vaut *max(event_time vu) − 2 min*. Les retards de 0, 30 et 90 s restent sous cette "
      f"limite : ils sont **comptés**. Les retards de 10 min tombent avant le watermark : ils sont "
      f"**ignorés** : {len(RETARDS) - comptes_wm} sur les {retard.get(600, 0)} générés avec 10 min de retard. "
      "Ceux du tout premier batch passent encore, car le watermark n'avance qu'à la fin d'un batch. La "
      f"métrique `numRowsDroppedByWatermark` affiche {ignores} et non le nombre d'événements : elle compte "
      "les lignes après l'**agrégation partielle** du micro-batch (une par fenêtre et par machine), "
      "pas les événements bruts. L'état se "
      "**stabilise**, car Spark supprime les fenêtres plus anciennes que le watermark. Sans watermark, "
      "chaque nouvelle fenêtre reste en mémoire pour toujours : sur un flux de plusieurs jours, l'état "
      "grossirait sans limite jusqu'à saturer la mémoire.", "")
    for q in spark.streams.active:
        q.stop()

    # Q4 - checkpoint
    R("## Q4. Checkpoint et reprise", "")
    sortie, ck = IOT / "sortie", IOT / "ck_sortie"
    q = flux.writeStream.format("parquet").option("path", str(sortie)).option("checkpointLocation", str(ck)).start()
    while not q.recentProgress or q.lastProgress["batchId"] < 4:
        time.sleep(0.5)
    q.stop()
    avant = spark.read.parquet(str(sortie)).count()
    q = flux.writeStream.format("parquet").option("path", str(sortie)).option("checkpointLocation", str(ck)).start()
    q.processAllAvailable()
    q.stop()
    apres = spark.read.parquet(str(sortie))
    n_total, n_distinct = apres.count(), apres.select("event_id").distinct().count()
    q = (flux.writeStream.format("parquet").option("path", str(IOT / "sortie_neuve"))
         .option("checkpointLocation", str(IOT / "ck_neuf")).start())
    q.processAllAvailable()
    q.stop()
    neuf = spark.read.parquet(str(IOT / "sortie_neuve")).count()
    ck_agg = str(IOT / "ck_agg")
    q = (flux.withWatermark("event_time", "2 minutes").groupBy(F.window("event_time", "1 minute"), "machine_id")
         .count().writeStream.format("memory").queryName("agg_ck").outputMode("update")
         .option("checkpointLocation", ck_agg).start())
    q.processAllAvailable()
    q.stop()
    try:
        q = (flux.withWatermark("event_time", "2 minutes").groupBy(F.window("event_time", "5 minutes"), "machine_id")
             .agg(F.avg("temperature")).writeStream.format("memory").queryName("agg_ck2").outputMode("update")
             .option("checkpointLocation", ck_agg).start())
        q.processAllAvailable()
        q.stop()
        erreur = "aucune erreur"
    except Exception as e:  # noqa: BLE001
        erreur = str(e).split("\n")[0][:260]
    R.tableau(["Scénario", "Résultat"], [
        ["Arrêt après le batch 4", f"{avant} lignes écrites"],
        ["Relance avec **le même** checkpoint", f"{n_total} lignes, {n_distinct} `event_id` distincts → "
         + ("**ni doublon ni trou**" if n_total == n_distinct == len(RETARDS) else "écart à analyser")],
        ["Relance avec un checkpoint **neuf** (autre sortie)", f"{neuf} lignes : tout est retraité depuis le début"],
        ["Fenêtre 1 min → 5 min, même checkpoint", f"`{erreur}`"]])
    R("Le checkpoint mémorise les **offsets**, c'est-à-dire les fichiers déjà lus, les batchs commis "
      "et l'**état** des agrégations. Après un arrêt, la requête reprend exactement au batch suivant. "
      "Le sink fichier tient en plus son propre journal (`_spark_metadata`), ce qui garantit "
      "l'exactly-once. Un checkpoint neuf ne sait rien du passé et relit tout. Changer la forme de "
      "l'agrégation rend l'**état stocké incompatible** avec la nouvelle requête : Spark refuse de "
      "repartir. Il faut alors un nouveau checkpoint.", "")

    # Q5 - foreachBatch
    R("## Q5. foreachBatch idempotent", "")
    alertes = IOT / "alertes"

    def sauver(batch_df, batch_id):
        (batch_df.groupBy("machine_id").agg(F.round(F.avg("temperature"), 1).alias("temp_moy"))
         .filter("temp_moy > 80").withColumn("batch_id", F.lit(batch_id))
         .write.mode("overwrite").parquet(str(alertes / f"batch_id={batch_id}")))

    q = flux.writeStream.foreachBatch(sauver).option("checkpointLocation", str(IOT / "ck_alertes")).start()
    q.processAllAvailable()
    q.stop()
    al = spark.read.parquet(str(alertes))
    cible = al.groupBy("batch_id").count().orderBy(F.desc("count")).first()["batch_id"]
    avant_rejeu = al.filter(F.col("batch_id") == cible).count()
    # Rejeu simulé : avec maxFilesPerTrigger = 1, le batch k a lu le fichier b{k:03d}.json
    sauver(spark.read.schema(SCHEMA).json(str(ENTREE / f"b{cible:03d}.json")), cible)
    al = spark.read.parquet(str(alertes))
    apres_rejeu = al.filter(F.col("batch_id") == cible).count()
    R(f"{al.select('batch_id').distinct().count()} dossiers `batch_id=…` écrits sur {NB_FICHIERS} micro-batchs, "
      f"soit {al.count()} alertes au total (machines dont la moyenne dépasse 80 °C dans le micro-batch). "
      f"On rejoue ensuite le batch {cible} : il avait {avant_rejeu} alerte(s) avant le rejeu et en a "
      f"**{apres_rejeu}** après. La partition `batch_id={cible}` a été **écrasée**, pas complétée.", "",
      "Après une panne, Spark peut **rejouer** le dernier micro-batch non commis dans le checkpoint. "
      "`foreachBatch` exécute du code libre, sans la garantie exactly-once du sink fichier. Une écriture "
      "en `append` produirait donc des doublons. Le `batch_id` est **stable** entre deux exécutions d'un "
      "même batch. L'utiliser comme partition écrasée en `overwrite`, ou comme clé de déduplication, "
      "rend l'écriture **idempotente**.", "")

    # Q6 - déduplication + jointure
    R("## Q6. Déduplication et jointure flux-statique", "")
    machines = spark.createDataFrame([("M1", "Limoges", "Atelier A"), ("M2", "Limoges", "Atelier B"),
                                      ("M3", "Brive", "Atelier C")], ["machine_id", "site", "atelier"])
    doublon = ENTREE / "b999.json"                         # fichier rejoué par la source amont
    doublon.write_text((ENTREE / "b000.json").read_text())
    enrichi = (flux.withWatermark("event_time", "2 minutes").dropDuplicates(["event_id"])
               .join(machines, "machine_id"))
    q = enrichi.writeStream.format("memory").queryName("enrichi").outputMode("append").start()
    q.processAllAvailable()
    p6 = plan_streaming(q)
    q.stop()
    lignes = spark.table("enrichi")
    R(f"Un fichier déjà reçu (`b000.json`) est redéposé sous un autre nom. Après `dropDuplicates(['event_id'])` "
      f"avec watermark, on obtient {lignes.count()} lignes pour {lignes.select('event_id').distinct().count()} "
      "`event_id` distincts. Les 20 doublons sont éliminés, et les événements en retard de plus de 2 min "
      "sont exclus par le watermark.", "")
    R.tableau(["machine_id", "site", "atelier", "événements"],
              [[r["machine_id"], r["site"], r["atelier"], r["count"]]
               for r in lignes.groupBy("machine_id", "site", "atelier").count().orderBy("machine_id").collect()])
    jointure = next((j for j in ["BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin"] if j in p6), "?")
    R(f"Plan physique de la requête : **{jointure}** pour la jointure flux-statique et "
      f"**{'StreamingDeduplicate' if 'StreamingDeduplicate' in p6 else 'déduplication'}** avec état. "
      "La table `machines` est petite et statique : elle est diffusée à chaque micro-batch, et "
      "**seul le flux garde un état**, celui des `event_id` vus, purgé par le watermark.", "",
      "Une **jointure externe flux-flux** doit savoir quand une ligne n'aura *jamais* de "
      "correspondance pour émettre le NULL. Sans condition de temps entre les deux flux et sans "
      "watermark, cette réponse n'arrive jamais : Spark devrait garder **tout** l'historique des deux "
      "côtés, soit un état infini. Il refuse donc ces requêtes.", "")
    R("## Livrable — `lastProgress` commenté (requête avec watermark)", "")
    R.code(json.dumps({k: q_wm.lastProgress[k] for k in ["batchId", "numInputRows", "eventTime", "stateOperators"]},
                      indent=2, default=str)[:2500], "json")
    R("`eventTime.watermark` indique la limite courante, à 2 minutes sous le maximum observé. "
      "`stateOperators[0].numRowsTotal` donne la taille de l'état : elle reste bornée. "
      "`numRowsDroppedByWatermark` compte les événements trop tardifs.", "")
    R.sauver("ex6_streaming.md")
    spark.stop()


if __name__ == "__main__":
    main()
