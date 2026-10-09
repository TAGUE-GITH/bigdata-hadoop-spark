# TP 5 — Hadoop Investigation Challenge : fiches d'incident

Méthode appliquée à chaque incident : **symptôme → au moins deux hypothèses → une commande par
hypothèse → cause prouvée → correction minimale → validation → prévention**. Aucune action
destructrice n'est faite sans preuve : pas de reformatage, et pas de `-safemode leave` à l'aveugle.

---

### Incident A — `hadoop: command not found`

| Champ | Contenu |
|---|---|
| Symptôme | `bash: hadoop: command not found` dans un nouveau terminal, alors que la commande marchait avant |
| Hypothèses | H1 : `PATH` ne contient pas `$HADOOP_HOME/bin`. H2 : `HADOOP_HOME` est vide ou faux. H3 : Hadoop n'est pas installé ou le dossier a disparu (Colab réinitialisé) |
| Preuves | `echo $HADOOP_HOME` → vide ; `echo $PATH \| tr : '\n' \| grep hadoop` → rien ; `ls /content/hadoop-3.5.0/bin/hadoop` → présent |
| Cause racine | Les variables étaient définies dans une session précédente (`%env`, `export`), pas dans `~/.bashrc` : un nouveau shell ne les hérite pas |
| Correction | Ajouter `export HADOOP_HOME=/content/hadoop-3.5.0` et `export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH` dans `~/.bashrc`, puis lancer `source ~/.bashrc` |
| Validation | `hadoop version` affiche Hadoop 3.5.0 ; `which hadoop` donne le bon chemin |
| Prévention | Une cellule d'initialisation unique (cellule 0) rejouée à chaque session, et un contrôle `hadoop version` en tête de script |

### Incident B — DataNode absent dans `jps`

| Champ | Contenu |
|---|---|
| Symptôme | `jps` montre NameNode et SecondaryNameNode, mais pas de DataNode ; `dfsadmin -report` : `Live datanodes (0)` |
| Hypothèses | H1 : **clusterID incompatible** après un reformatage du NameNode. H2 : répertoire `dfs.datanode.data.dir` absent ou sans droits. H3 : port 9866 déjà utilisé |
| Preuves | `tail -n 50 $HADOOP_HOME/logs/hadoop-*-datanode-*.log` → `Incompatible clusterIDs in /content/hdfs/data: namenode clusterID = CID-aaa; datanode clusterID = CID-bbb` |
| Cause racine | Le NameNode a été reformaté (nouveau clusterID), mais le DataNode garde l'ancien dans `data/current/VERSION` |
| Correction | Sur un environnement **de TP sans données à préserver** : arrêter HDFS, vider `/content/hdfs/data`, puis relancer `start-dfs.sh`. En production, on recopie plutôt le clusterID dans `VERSION` pour garder les blocs |
| Validation | `jps` liste le DataNode ; `hdfs dfsadmin -report` → `Live datanodes (1)` |
| Prévention | Ne jamais reformater un NameNode existant (`-format` seulement à l'installation) ; superviser le nombre de DataNodes vivants |

### Incident C — Safe mode actif

| Champ | Contenu |
|---|---|
| Symptôme | `Cannot create directory /data. Name node is in safe mode.` |
| Hypothèses | H1 : démarrage récent, le NameNode attend les *block reports* (normal, transitoire). H2 : des blocs manquent (DataNode mort, disque perdu), donc le seuil de 99,9 % n'est pas atteint. H3 : safe mode activé manuellement |
| Preuves | `hdfs dfsadmin -safemode get` → ON ; `hdfs dfsadmin -report` → nombre de DataNodes vivants ; `hdfs fsck / \| tail` → `Missing blocks`, `CORRUPT` ; log du NameNode : `The reported blocks X needs additional Y blocks to reach the threshold 0.999` |
| Cause racine | À établir avec les preuves. Le cas typique de TP est le DataNode arrêté (incident B), donc des blocs non signalés |
| Correction | **Corriger la cause** (redémarrer le DataNode) : le safe mode se lève seul quand le seuil est atteint. `-safemode leave` uniquement si les blocs manquants sont connus et acceptés, puis `fsck -delete` en dernier recours |
| Validation | `-safemode get` → OFF ; `hdfs dfs -mkdir /test_ecriture` réussit ; `fsck /` → HEALTHY |
| Prévention | Alerte sur les blocs manquants et sur les DataNodes morts. Procédure écrite : « diagnostiquer avant toute sortie du safe mode » |

### Incident D — `FileAlreadyExistsException`

| Champ | Contenu |
|---|---|
| Symptôme | Le job MapReduce échoue avant de démarrer : `Output directory hdfs://localhost:9000/tp_avance/ca/output already exists` |
| Hypothèses | H1 : la sortie d'une exécution précédente existe. H2 : faute de frappe, la sortie pointe vers un dossier existant (l'entrée par exemple) |
| Preuves | `hdfs dfs -test -e /tp_avance/ca/output; echo $?` → 0 ; `hdfs dfs -ls /tp_avance/ca/output` → `_SUCCESS`, `part-00000` |
| Cause racine | Hadoop refuse d'écraser une sortie existante, pour protéger un résultat précédent |
| Correction | Vérifier que l'ancien résultat n'est plus utile, puis `hdfs dfs -rm -r -f /tp_avance/ca/output`, ou utiliser une sortie horodatée `/output/run=AAAAMMJJ-HHMM` |
| Validation | Le job se termine avec `completed successfully` ; `hdfs dfs -cat .../part-*` affiche les 3 pays |
| Prévention | Un script de lancement qui supprime ou horodate la sortie (cf. `tp3_mapreduce/run_hadoop.sh`) |

### Incident E — `Permission denied` dans HDFS

| Champ | Contenu |
|---|---|
| Symptôme | `put: Permission denied: user=etudiant, access=WRITE, inode="/data/curated":root:supergroup:drwxr-x---` |
| Hypothèses | H1 : droits insuffisants (750 : le groupe n'écrit pas). H2 : mauvais propriétaire. H3 : l'utilisateur n'est pas dans le groupe attendu |
| Preuves | Le message donne lui-même propriétaire, groupe et mode ; `hdfs dfs -ls -d /data/curated` ; `hdfs groups etudiant` |
| Cause racine | `/data/curated` est en 750 et appartient à `root:supergroup` : l'utilisateur ne peut pas écrire |
| Correction | Correction **minimale** : écrire dans la zone prévue, ou `hdfs dfs -chown -R etudiant:data /data/curated/...` sur le sous-dossier concerné. Jamais `chmod -R 777 /` |
| Validation | `hdfs dfs -put` réussit ; `-ls` montre le bon propriétaire |
| Prévention | Matrice des droits par zone (raw, curated, analytics) documentée et appliquée par script ; ACL HDFS si besoin |

### Incident F — Mapper non exécutable

| Champ | Contenu |
|---|---|
| Symptôme | Le job échoue : `PipeMapRed.waitOutputThreads(): subprocess failed with code 126` (ou 127) |
| Hypothèses | H1 : `mapper.py` sans bit d'exécution, lancé avec `-mapper mapper.py`. H2 : shebang absent ou fins de ligne Windows (`python3\r`). H3 : fichier non envoyé avec `-files` |
| Preuves | `ls -l mapper.py` → `-rw-r--r--` ; `head -1 mapper.py \| od -c` → `\r` ; `yarn logs -applicationId <id> \| grep -i "permission\|not found"` |
| Cause racine | Le script n'est pas exécutable et la commande `-mapper` l'appelle directement |
| Correction | `chmod +x mapper.py reducer.py`, ou passer l'interpréteur explicitement : `-mapper "python3 mapper.py"`. Convertir avec `dos2unix` si nécessaire |
| Validation | Test local `cat ventes.csv \| ./mapper.py` OK, puis job relancé avec succès |
| Prévention | Toujours `-mapper "python3 mapper.py"` et `-files` ; test local obligatoire avant soumission ; `.gitattributes` avec `*.py eol=lf` |

### Incident G — Aucun fichier trouvé

| Champ | Contenu |
|---|---|
| Symptôme | `put: '/content/ventes.csv': No such file or directory` ou `Input path does not exist: hdfs://.../input` |
| Hypothèses | H1 : confusion entre chemin **local** et chemin **HDFS**. H2 : le glob `*.csv` n'est développé par personne (guillemets) ou ne correspond à rien. H3 : faute de frappe ou mauvais répertoire courant |
| Preuves | `ls -l /content/ventes.csv` (local) ; `hdfs dfs -ls /tp_avance/ca/input` (HDFS) ; `hdfs dfs -test -e <chemin>; echo $?` ; `pwd` |
| Cause racine | Exemple type : le fichier a été chargé dans `/tp_avance/ca/inputs/`, alors que le job lit `/input` |
| Correction | Recharger au bon endroit ou corriger le `-input` |
| Validation | `hdfs dfs -ls` montre le fichier attendu ; le job lit le bon nombre de lignes (`Map input records`) |
| Prévention | Chemins dans des variables en tête de script ; `hdfs dfs -test -e` avant le job, avec arrêt explicite si absent |

### Incident H — Résultat incorrect

| Champ | Contenu |
|---|---|
| Symptôme | Le job réussit mais FR vaut par exemple 76,00 au lieu de 66,00, ou un pays apparaît deux fois |
| Hypothèses | H1 : une ligne invalide passe (la quantité `-1` est comptée). H2 : l'en-tête produit une sortie. H3 : le reducer suppose des clés triées alors qu'il reçoit des clés avec espaces ou casse différente (`FR` / `FR `). H4 : plusieurs reducers sans tri global, mal interprétés |
| Preuves | Comparer **entrée** (10 lignes), **compteurs** (Map input 10, Map output 6, groupes 3), **sortie du mapper** (test local `cat \| mapper`), **tri** (`sort`) et **reducer** séparément |
| Cause racine | Exemple : le mapper teste `q < 0` au lieu de `q <= 0`, ou ne nettoie pas les espaces. Les compteurs `Valeur_negative = 0` le trahissent |
| Correction | Corriger la règle dans le mapper, puis rejouer le test local avant le job |
| Validation | Résultat BE 55,00, ES 30,00, FR 66,00, et 10 lues = 1 en-tête + 6 valides + 3 rejetées |
| Prévention | Tests unitaires (`tp3_mapreduce/test_mapreduce.py`) et réconciliation automatique des compteurs à chaque exécution |

---

## Conclusion — observabilité et rejeu

Les huit incidents se résolvent **avec des preuves**, pas par essais successifs :

- les journaux (`$HADOOP_HOME/logs`, `yarn logs`) donnent la cause exacte (B, F) ;
- les messages d'erreur contiennent souvent la réponse (E) ;
- les **compteurs** relient l'entrée à la sortie et révèlent un résultat faux même quand le job « réussit » (H).

L'**observabilité** (logs, métriques, compteurs métier, `fsck`, `dfsadmin -report`) raccourcit le
diagnostic. La capacité de **rejeu** permet de corriger sans peur et de prouver que la correction
fonctionne : données brutes immuables, sorties horodatées, scripts idempotents, tests locaux.
