#!/usr/bin/env bash
# Mission C : simulation locale du job (cat | mapper | sort | reducer), sans Hadoop.
set -euo pipefail
cd "$(dirname "$0")"
PY=${PYTHON:-python3}
mkdir -p sorties
cat ventes.csv | $PY mapper.py 1> sorties/sortie_map.txt 2> sorties/erreurs_map.txt
cat ventes.csv | $PY mapper.py 2>/dev/null | LC_ALL=C sort | $PY reducer.py 2>/dev/null > sorties/resultat.txt
echo "=== Sortie du Mapper ==="; cat sorties/sortie_map.txt
echo "=== Compteurs du Mapper ==="; sort sorties/erreurs_map.txt | uniq -c
echo "=== Résultat final (après sort + reducer) ==="; cat sorties/resultat.txt
