#!/usr/bin/env bash
# TP 4 Hadoop - Reproduire le problème des petits fichiers, puis compacter.
set -euo pipefail

echo "=== Mission A : 200 petits fichiers ==="
mkdir -p ~/tp4-smallfiles/source
for i in $(seq -w 1 200); do
  echo "2026-10-11;FR;P01;$i" > ~/tp4-smallfiles/source/event_$i.csv
done
hdfs dfs -mkdir -p /tp4/raw/events
hdfs dfs -put -f ~/tp4-smallfiles/source/*.csv /tp4/raw/events/
hdfs dfs -count -q -h /tp4/raw/events          # DIR_COUNT FILE_COUNT CONTENT_SIZE
hdfs dfs -du -s -h /tp4/raw/events
hdfs fsck /tp4/raw/events | grep -E "Total files|Total blocks"

echo "=== Mission B : compaction ==="
cat ~/tp4-smallfiles/source/*.csv > ~/tp4-smallfiles/events_compactes.csv
hdfs dfs -mkdir -p /tp4/curated/events
hdfs dfs -put -f ~/tp4-smallfiles/events_compactes.csv /tp4/curated/events/
hdfs dfs -count -q -h /tp4/curated/events
hdfs fsck /tp4/curated/events/events_compactes.csv -files -blocks -locations | grep -E "Total files|Total blocks|len="
