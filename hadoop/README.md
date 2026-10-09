# Hadoop — TP 1 à 6

| TP | Thème | Ce qui a été exécuté | Livrables |
|---|---|---|---|
| [1](tp1_architecte_hdfs) | Architecte HDFS : blocs, réplication, panne | calculs et simulation de la panne et de la re-réplication (Python) | [TP1_resultats.md](tp1_architecte_hdfs/TP1_resultats.md) |
| [2](tp2_admin_hdfs) | Administrateur HDFS : zones, droits, `_READY` | contrôle qualité des 4 fichiers (pandas) | [README](tp2_admin_hdfs/README.md), [controle_qualite.md](tp2_admin_hdfs/controle_qualite.md), [tp2_hdfs.sh](tp2_admin_hdfs/tp2_hdfs.sh) |
| [3](tp3_mapreduce) | MapReduce Streaming : CA par pays et compteurs qualité | mapper, sort et reducer en local, plus 4 tests automatiques | [README](tp3_mapreduce/README.md), [mapper.py](tp3_mapreduce/mapper.py), [reducer.py](tp3_mapreduce/reducer.py) |
| [4](tp4_petits_fichiers) | Petits fichiers, compaction, formats, partitionnement | mesure Spark des petits fichiers (8 000 contre 1 000 fichiers) | [README](tp4_petits_fichiers/README.md) |
| [5](tp5_investigation) | Investigation : 8 incidents | — (fiches de diagnostic) | [8 fiches](tp5_investigation/README.md) |
| [6](tp6_ecosysteme) | Écosystème : Hive, HBase, Pig, Flume, ZooKeeper, Spark | job Spark sur les ventes, recoupé avec pandas | [architecture + pipeline](tp6_ecosysteme/README.md), [HiveQL](tp6_ecosysteme/hive_ventes.sql) |

**Commandes HDFS et YARN** : ce poste n'a ni WSL ni Docker, donc HDFS n'y tourne pas. Toutes les
commandes sont réunies dans le notebook [`colab/Hadoop_TP1_a_4_Colab.ipynb`](colab/Hadoop_TP1_a_4_Colab.ipynb).
Il installe Hadoop 3.5.0 en pseudo-distribué, clone ce dépôt et lance les scripts des TP 1 à 4.
Les valeurs indiquées comme « attendues » dans les comptes rendus sont les résultats logiques
déduits des calculs et des tests locaux.
