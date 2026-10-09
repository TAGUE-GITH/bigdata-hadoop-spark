# TP 1 — Architecte HDFS : capacité, réplication et panne

| Fichier | Contenu |
|---|---|
| [`calculs_hdfs.py`](calculs_hdfs.py) | Dimensionnement, placement équilibré, simulation de la panne de DN3 et de la re-réplication, bonus |
| [`TP1_resultats.md`](TP1_resultats.md) | **Page de calculs, tableaux avant et après panne, conclusion** (générés par le script) |
| [`../colab/Hadoop_TP1_a_4_Colab.ipynb`](../colab/Hadoop_TP1_a_4_Colab.ipynb) | Partie A : fichier de 300 Mo, `fsck`, sous-réplication, contrôle md5 |

## Résumé

| | 1 200 Mo, réplication 3 |
|---|---|
| Blocs logiques | **10** (9 × 128 Mo + 48 Mo) |
| Copies physiques | **30**, soit 5 par DataNode |
| Volume physique | **3 600 Mo** |
| Panne de DN3 | 5 blocs sous-répliqués, 0 indisponible, 5 copies recréées |
| Réplication 2 | 2 400 Mo (−33 %), mais une seule copie restante après une panne |

## Partie B — cluster réel à 6 DataNodes (Docker)

Cette partie demande Docker Desktop et le dossier `hadoop-6dn` fourni par l'enseignant, ce que ce
dépôt ne contient pas. Une fois le cluster lancé :

```bash
docker compose up -d && docker compose ps
docker compose exec namenode hdfs dfsadmin -report                    # Live datanodes (6)
docker compose exec namenode bash -lc "dd if=/dev/zero of=/tmp/logs_1200Mo.bin bs=1M count=1200"
docker compose exec namenode hdfs dfs -mkdir -p /tp1/input
docker compose exec namenode hdfs dfs -Ddfs.blocksize=134217728 -Ddfs.replication=3 -put /tmp/logs_1200Mo.bin /tp1/input/
docker compose exec namenode hdfs fsck /tp1/input/logs_1200Mo.bin -files -blocks -locations   # placement réel
docker compose stop datanode3                                          # panne
docker compose exec namenode hdfs dfs -cat /tp1/input/logs_1200Mo.bin > /dev/null; echo $?   # 0 = lisible
docker compose exec namenode hdfs fsck /tp1/input/logs_1200Mo.bin -files -blocks -locations   # Live_repl 2 puis 3
docker compose start datanode3
```

On compare ensuite le placement réel au tableau théorique de [`TP1_resultats.md`](TP1_resultats.md).
Le placement réel est choisi par le NameNode, en tenant compte des racks et de l'espace libre : il
sera différent, mais il respecte les mêmes invariants. Il n'y a jamais deux copies d'un bloc sur le
même nœud, et chaque bloc reste lisible après la perte d'un DataNode.
