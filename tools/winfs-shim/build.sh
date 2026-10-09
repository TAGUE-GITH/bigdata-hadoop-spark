#!/usr/bin/env bash
# Compile winfs-shim.jar contre le hadoop-client-api embarqué dans PySpark.
set -euo pipefail
cd "$(dirname "$0")"
JARS=$(python -c "import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
CP=$(ls "$JARS"/hadoop-client-api-*.jar | head -1)
rm -rf classes && mkdir -p classes
javac --release 17 -cp "$CP" -d classes src/fr/tague/winfs/*.java
jar cf winfs-shim.jar -C classes .
rm -rf classes
echo "winfs-shim.jar reconstruit"
