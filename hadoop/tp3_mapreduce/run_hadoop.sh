#!/usr/bin/env bash
# Mission D : exécution réelle sur Hadoop (Colab ou cluster), depuis ce dossier.
set -euo pipefail
cd "$(dirname "$0")"
chmod +x mapper.py reducer.py
hdfs dfs -mkdir -p /tp_avance/ca/input
hdfs dfs -put -f ventes.csv /tp_avance/ca/input/
STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" -name 'hadoop-streaming-*.jar' | head -1)
hdfs dfs -rm -r -f /tp_avance/ca/output          # sinon : FileAlreadyExistsException (Mission E, Q22)
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name=TP_Avance_CA_Qualite \
  -D mapreduce.job.reduces=1 \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /tp_avance/ca/input \
  -output /tp_avance/ca/output 2>&1 | tee execution.log
hdfs dfs -cat /tp_avance/ca/output/part-*
grep -E "Map input records|Map output records|Reduce input groups|Reduce output records|Qualite|Lignes_|Pays_|Quantite_|Valeur_|Entete_|Groupes_" execution.log
