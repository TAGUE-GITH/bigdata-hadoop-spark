# TP 4 — Performance HDFS : petits fichiers, formats et partitionnement

Script : [`tp4_smallfiles.sh`](tp4_smallfiles.sh) (missions A et B). La mesure sur Spark des petits fichiers
produits par `partitionBy` est dans [`../../spark/resultats/ex5_formats_optimiseur.md`](../../spark/resultats/ex5_formats_optimiseur.md).

## Mission A — Le problème

200 fichiers d'une ligne, d'environ 20 octets chacun :

| Indicateur | Valeur |
|---|---:|
| Fichiers (`hdfs dfs -count`) | 200 |
| Volume total (`hdfs dfs -du`) | environ 4 Ko |
| Blocs | 200, un par fichier, même pour 20 octets |
| Objets dans la mémoire du NameNode | **≈ 400** (200 fichiers + 200 blocs) |

Le NameNode garde **en RAM** un objet par fichier, par répertoire et par bloc, d'environ 150 octets
chacun. Le coût dépend du **nombre d'objets**, pas du volume : 4 Ko de données coûtent autant de
métadonnées que 200 fichiers de 128 Mo, soit 25 Go.

Extrapolation : un fichier par minute pendant un an donne 525 600 fichiers, soit environ 1 million
d'objets (≈ 150 Mo de heap NameNode) pour quelques Mo de données. Côté traitement, chaque fichier
devient une tâche map, ou au minimum une ouverture de fichier. On obtient 525 600 tâches de
quelques millisecondes, où le coût de lancement domine le calcul.

## Mission B — Compaction

| | Avant | Après `cat *.csv > events_compactes.csv` |
|---|---:|---:|
| Fichiers | 200 | **1** |
| Blocs | 200 | **1** |
| Objets NameNode | ≈ 400 | **2** |
| Tâches map pour une lecture | 200 | 1 |

Gain : la charge mémoire du NameNode est divisée par 200, les listings et ouvertures aussi. Côté
traitement, on passe de 200 tâches minuscules à quelques tâches de taille utile.

## Mission C — Partitionnement

```
/data/curated/events/year=2026/month=10/day=11/country=FR/part-0000.parquet
```

1. **Clés** : la date (`year/month/day`) et le pays, c'est-à-dire les deux filtres les plus
   fréquents des consultations.
2. **Ordre** : du filtre le plus utilisé et le plus grossier vers le plus fin. La date d'abord, car
   presque toutes les requêtes bornent une période et l'archivage se fait par date. Le pays
   ensuite. Une requête « octobre, FR » ne lit qu'un sous-arbre.
3. **Partitions trop fines** : `hour=` ou `minute=`, ou une clé à forte cardinalité comme
   `client_id`, recréent le problème des petits fichiers. On obtient un dossier et au moins un
   fichier par combinaison, souvent quasi vides. La règle : une partition doit contenir assez de
   données pour quelques fichiers de taille saine. Sinon, on remonte d'un niveau, au jour ou au mois.
4. **Formats** :

| Format | Structure | Compression | Schéma | Usage dans ce cas |
|---|---|---|---|---|
| CSV gzip | texte, ligne | bonne, mais **non découpable** (1 fichier = 1 tâche) | aucun | échange, archive brute |
| Avro | binaire, **ligne** | bonne, découpable | embarqué, évolutif | ingestion, messages Kafka, zone raw |
| **Parquet** | binaire, **colonnaire** | excellente (encodage par colonne) | embarqué | **analytique Spark/Hive : recommandé** |
| ORC | binaire, colonnaire | excellente, index intégrés | embarqué | analytique Hive, transactions ACID Hive |

## Mission D — Recommandation (15 lignes)

1. Il ne faut **pas** conserver un fichier par minute dans la zone exploitée : l'ingestion peut
   l'être, l'analyse non.
2. Les fichiers par minute arrivent dans `raw/`, puis une **compaction** les regroupe en
   `curated/`, partitionné par `year/month/day/country`.
3. La compaction se déclenche à la **clôture d'une partition**, par exemple chaque jour à J+1 pour
   la veille. Elle se déclenche aussi dès que le nombre de fichiers d'une partition dépasse un seuil.
4. Format recommandé pour l'analyse : **Parquet**, avec compression snappy. Il est colonnaire,
   compressé, porte son schéma et permet le predicate pushdown. Dans l'exercice Spark 5, il est
   4 fois plus petit que le CSV et environ 5 fois plus rapide à relire.
5. Taille cible : **des fichiers de l'ordre de la taille de bloc**, soit quelques centaines de Mo,
   ou au minimum plusieurs dizaines. La valeur se choisit selon le volume quotidien réel et le
   parallélisme voulu : il faut assez de fichiers pour occuper les cœurs, mais pas plus.
6. Contrôles à automatiser, par partition : nombre de fichiers, taille moyenne et minimale des
   fichiers, nombre d'objets du NameNode (`hdfs dfs -count`), présence de `_READY`, et durée de la
   dernière compaction.
7. Une alerte se déclenche si la taille moyenne d'une partition clôturée passe sous un seuil, par
   exemple 32 Mo.
