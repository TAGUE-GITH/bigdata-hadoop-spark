#!/usr/bin/env bash
# TP 2 Hadoop - Administrateur HDFS : organiser, charger, inspecter et sécuriser /data.
# À exécuter depuis ce dossier sur une machine où HDFS est démarré (Colab ou cluster).
set -euo pipefail
cd "$(dirname "$0")"
DATE_INGESTION=2026-10-11
mkdir -p preuves

echo "=== Mission A : services ==="
jps | tee preuves/jps.txt
hdfs dfsadmin -report | tee preuves/dfsadmin_report.txt | grep -E "Live datanodes|Dead datanodes"
yarn node -list 2>/dev/null | tee preuves/yarn_nodes.txt || echo "YARN non démarré"

echo "=== Mission B : arborescence ==="
for zone in sales customers products web_logs; do
  hdfs dfs -mkdir -p /data/raw/$zone/ingestion_date=$DATE_INGESTION
done
hdfs dfs -mkdir -p /data/curated/sales/year=2026/month=10 /data/analytics /data/rejects

echo "=== Mission C : chargement et inspection ==="
hdfs dfs -put -f data/ventes.csv    /data/raw/sales/ingestion_date=$DATE_INGESTION/
hdfs dfs -put -f data/clients.csv   /data/raw/customers/ingestion_date=$DATE_INGESTION/
hdfs dfs -put -f data/produits.csv  /data/raw/products/ingestion_date=$DATE_INGESTION/
hdfs dfs -put -f data/logs_web.csv  /data/raw/web_logs/ingestion_date=$DATE_INGESTION/
hdfs dfs -ls -R /data | tee preuves/arborescence.txt
hdfs dfs -du -h /data/raw | tee preuves/du_raw.txt
hdfs dfs -cat /data/raw/sales/ingestion_date=$DATE_INGESTION/ventes.csv | head -5
for f in sales/ingestion_date=$DATE_INGESTION/ventes.csv web_logs/ingestion_date=$DATE_INGESTION/logs_web.csv; do
  hdfs fsck /data/raw/$f -files -blocks -locations | tee -a preuves/fsck.txt
done

echo "=== Tableau comparatif local / HDFS ==="
printf "%-14s %-40s %10s | %-70s %10s %4s\n" Fichier "Chemin local" Octets "Chemin HDFS" Octets Repl
for couple in ventes:sales clients:customers produits:products logs_web:web_logs; do
  f=${couple%%:*}; z=${couple##*:}
  hdfs_path=/data/raw/$z/ingestion_date=$DATE_INGESTION/$f.csv
  printf "%-14s %-40s %10s | %-70s %10s %4s\n" "$f.csv" "$(pwd)/data/$f.csv" "$(stat -c %s data/$f.csv)" \
    "$hdfs_path" "$(hdfs dfs -stat %b "$hdfs_path")" "$(hdfs dfs -stat %r "$hdfs_path")"
done | tee preuves/comparatif_local_hdfs.txt

echo "=== Mission D : droits et cycle de vie ==="
hdfs dfs -chmod 750 /data/curated
hdfs dfs -chmod 770 /data/analytics
hdfs dfs -touchz /data/analytics/_READY
hdfs dfs -ls /data | tee preuves/droits.txt
hdfs dfs -stat "%n %b %r" /data/analytics/_READY
