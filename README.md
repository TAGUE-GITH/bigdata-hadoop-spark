# Big Data — Hadoop & Spark

Travaux pratiques de traitement massif : **6 TP Hadoop** (HDFS, administration, MapReduce,
performance, diagnostic, écosystème) et **6 exercices Spark** (exécution, RDD, DataFrame et
qualité, jointures et skew, formats et optimiseur, streaming).

Tous les résultats Spark et les tests MapReduce ont été **exécutés réellement**. Les comptes rendus
sont générés par les scripts eux-mêmes, et chaque chiffre publié vient d'une exécution.

## Points saillants

| Sujet | Mesure |
|---|---|
| Skew de jointure (60 % des commandes sur un produit) | tâche max / médiane = **9,0**, ramené à 1,0 à 2,2 par broadcast, AQE, salage ou clé chaude séparée |
| Naïf contre optimisé (CA par pays et famille, 5 M lignes) | **15,2 s → 0,95 s (× 16)** avec Parquet, fonctions natives, broadcast et colonnes utiles |
| Formats | CSV 174,5 Mo · JSON 578,5 Mo · **Parquet 44,3 Mo** ; pour `avg(prix)`, Parquet ne lit que 8,4 Mo |
| Petits fichiers | `partitionBy('id_produit')` : **8 000 fichiers de 10 Ko** → 1 000 avec `repartition` d'abord |
| UDF Python contre natif | **10 fois plus lent** (`BatchEvalPython`) |
| Streaming | watermark 2 min : retards de 10 min ignorés, état borné ; reprise sur checkpoint sans doublon ni trou |
| MapReduce Streaming | CA par pays et compteurs qualité ; 10 lues = 1 en-tête + 6 valides + 3 rejetées |
| HDFS (1 200 Mo, réplication 3, 6 DataNodes) | 10 blocs, 30 copies, 3 600 Mo ; la panne d'un nœud laisse 5 blocs sous-répliqués et 0 perdu |

## Contenu

```
hadoop/
  tp1_architecte_hdfs/   calculs, placement, panne et re-réplication simulés
  tp2_admin_hdfs/        zones raw/curated/analytics/rejects, droits, _READY, contrôle qualité
  tp3_mapreduce/         mapper.py, reducer.py, test local, tests automatiques, job Streaming
  tp4_petits_fichiers/   compaction, partitionnement, choix de format
  tp5_investigation/     8 fiches d'incident (symptôme → preuve → correction → prévention)
  tp6_ecosysteme/        architecture (Mermaid), pipeline en 12 étapes, HiveQL, job Spark
  colab/                 notebook : Hadoop 3.5.0 pseudo-distribué + scripts des TP 1 à 4
spark/
  ex1 … ex6              scripts exécutables
  resultats/             comptes rendus générés (prédictions, mesures, explications)
tools/winfs-shim/        adaptateur Java pour exécuter Spark sous Windows sans winutils.exe
```

- Détail Hadoop : [`hadoop/README.md`](hadoop/README.md)
- Détail Spark : [`spark/README.md`](spark/README.md)

## Lancer

```bash
pip install -r requirements.txt
python spark/run_all.py                          # les 6 exercices Spark, environ 12 min
python hadoop/tp3_mapreduce/test_mapreduce.py    # tests du mapper et du reducer
bash hadoop/tp3_mapreduce/test_local.sh          # cat | mapper | sort | reducer
python hadoop/tp1_architecte_hdfs/calculs_hdfs.py
```

Pour les commandes HDFS et YARN, ouvrir
[`hadoop/colab/Hadoop_TP1_a_4_Colab.ipynb`](hadoop/colab/Hadoop_TP1_a_4_Colab.ipynb) dans Google Colab.

## Stack

PySpark 4.2.0 · Hadoop 3.5.0 (HDFS, YARN, MapReduce Streaming) · Hive · Python 3.13 · pandas · Java 21
