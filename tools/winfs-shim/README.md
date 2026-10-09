# winfs-shim — Spark sur Windows sans `winutils.exe`

Sous Windows, Hadoop (utilisé par Spark pour écrire des fichiers) appelle `winutils.exe` et
`hadoop.dll` pour gérer les permissions POSIX, lister les dossiers et lire les liens symboliques.
Sans ces binaires, `df.write.parquet(...)` et les checkpoints de streaming échouent avec
`HADOOP_HOME and hadoop.home.dir are unset`.

Plutôt que de télécharger des binaires tiers, ce module surcharge trois méthodes de
`RawLocalFileSystem` :

| Méthode | Comportement |
|---|---|
| `setPermission`, `setOwner` | ignorées : NTFS n'a pas de permissions POSIX |
| `listStatus` | listing via `java.io.File` au lieu de `NativeIO` |
| `getFileLinkStatus` | renvoie `getFileStatus`, sans appel à `readlink` |

Deux classes l'exposent : `NoPermLocalFileSystem` (API `FileSystem`, utilisée par les écritures
batch) et `NoPermLocalFs` (API `FileContext`, utilisée par les checkpoints de streaming). Elles sont
branchées dans `spark/common.py` :

```python
.config("spark.driver.extraClassPath", "tools/winfs-shim/winfs-shim.jar")
.config("spark.hadoop.fs.file.impl", "fr.tague.winfs.NoPermLocalFileSystem")
.config("spark.hadoop.fs.AbstractFileSystem.file.impl", "fr.tague.winfs.NoPermLocalFs")
```

Recompiler (JDK 17 ou plus récent) :

```bash
bash tools/winfs-shim/build.sh
```

> À utiliser uniquement en développement local. Sur Linux, macOS, Colab ou un cluster, le
> système de fichiers standard est utilisé, car le shim n'est activé que si `os.name == "nt"`.
