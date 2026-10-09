# Exercice 5 — Formats, stockage, cache et optimiseur

_Résultats générés par l'exécution réelle du script (PySpark 4.2.0, local, N = 5 000 000 commandes). Les durées dépendent de la machine._

## Q1. CSV, JSON et Parquet (snappy)

**Prédiction** : le Parquet sera 3 à 5 fois plus petit que le CSV et le plus rapide à relire. Le JSON sera le plus volumineux, car il répète le nom des colonnes sur chaque ligne.

| Format | Taille disque | Écriture | Lecture + avg(prix) | Volume à lire |
|---|---|---|---|---|
| CSV | 174.5 Mo | 6.2 s | 2.27 s | 175.9 Mo |
| JSON | 578.5 Mo | 3.1 s | 3.77 s | 583.3 Mo |
| PARQUET | 44.3 Mo | 2.2 s | 0.37 s | 8.4 Mo |

_Volume à lire : pour CSV et JSON, octets lus mesurés par Spark (le fichier entier) ; pour Parquet, taille compressée de la seule colonne `prix_unitaire`, d'après les métadonnées des fichiers._

Parquet est **colonnaire** : pour `avg(prix_unitaire)`, Spark ne lit que la colonne demandée (quelques Mo au lieu du fichier entier). Il est aussi **compressé** (snappy, plus un encodage par dictionnaire très efficace sur `pays` ou `id_produit`) et **typé** : le schéma est embarqué et il n'y a aucun texte à parser. Le CSV doit lire et découper chaque ligne entière. Le JSON répète en plus les noms de colonnes sur chaque ligne, c'est le plus lourd.

## Q2. Élagage de colonnes et predicate pushdown

| Source | ReadSchema | PushedFilters | Volume lu |
|---|---|---|---|
| Parquet | `struct<pays:string,quantite:int>` | `[IsNotNull(pays), EqualTo(pays,FR)]` | 4.7 Mo |
| CSV | `struct<pays:string,quantite:int>` | `[IsNotNull(pays), EqualTo(pays,FR)]` | 175.9 Mo |

Dans les deux cas, Catalyst réduit le `ReadSchema` à 2 colonnes et pousse le filtre vers la source. **Mais l'effet n'est pas le même.** Le lecteur Parquet ne lit physiquement que les colonnes `pays` et `quantite`, et utilise les statistiques min/max de chaque *row group* pour sauter les blocs sans « FR ». Le lecteur CSV doit quand même lire **tout le fichier texte** octet par octet : il ne parse que les colonnes utiles, mais le volume lu reste le fichier entier.

## Q3. `partitionBy` et petits fichiers

`partitionBy('pays')` crée **5 dossiers** (`pays=FR`, `pays=BE`…). Plan de la requête `pays = 'FR'` : `PartitionFilters: [isnotnull(pays#7608), (pays#7608 = FR)]`. Fichiers ouverts : 8.6 Mo (dossier `pays=FR`), contre 43.1 Mo pour tout le jeu. Seul le dossier `pays=FR` est ouvert : c'est le **partition pruning**.

| Écriture | Dossiers | Fichiers Parquet | Taille moyenne d'un fichier | Durée |
|---|---|---|---|---|
| `partitionBy('id_produit')` direct | 1000 | 8000 | 9.7 Ko | 55.2 s |
| `repartition('id_produit')` puis `partitionBy` | 1000 | 1000 | 53.1 Ko | 8.8 s |

Partitionner sur une colonne à **1 000 valeurs** crée 1 000 dossiers. Sans précaution, chacune des 8 tâches d'écriture produit un fichier dans chaque dossier où elle a des lignes, soit jusqu'à 8 000 petits fichiers de quelques Ko. Chaque fichier coûte un objet de métadonnées (sur HDFS, dans la mémoire du NameNode), une ouverture et un listing à chaque lecture. Un `repartition('id_produit')` avant l'écriture regroupe chaque produit dans une seule tâche : on obtient **un fichier par dossier**. `maxRecordsPerFile` sert au cas inverse, pour plafonner la taille des gros fichiers. Ici `pays` (5 valeurs) est une bonne clé de partition, mais `id_produit` (1 000 valeurs) ne l'est pas.

## Q4. Cache et persistance

| Variante | Durée des 3 KPI |
|---|---|
| sans cache | 3.71 s |
| cache() | 1.62 s |
| persist(MEMORY_AND_DISK) | 1.27 s |

Sans cache, chacun des 3 KPI relit le Parquet, refait la jointure et l'agrégation. Avec `cache()`, le premier KPI matérialise le résultat intermédiaire, qui apparaît dans l'onglet Storage, et les deux suivants le relisent. `unpersist()` libère la mémoire ensuite.

**Cas où le cache ne sert à rien** : un DataFrame utilisé une seule fois. On paie alors le stockage sans jamais le relire, et la mémoire prise peut forcer des déversements ailleurs. Autre cas : un `cache()` placé avant un filtre très sélectif, qui stocke des données que l'on va jeter.

**cache() ou checkpoint()** : le cache garde le **lignage**. Si une partition est perdue, Spark la recalcule. `checkpoint()` écrit le résultat sur disque, dans `setCheckpointDir`, et **coupe le lignage**. C'est utile pour des plans très longs, par exemple des itérations ou des jointures successives, dont l'analyse et la reprise deviendraient coûteuses.

```text
Avant checkpoint : +- *(1) Range (0, 10, step=1, splits=4)
Après checkpoint : *(1) Scan ExistingRDD[id#8649L,x#8650L]
```

## Q5. Lire Catalyst

`explain('extended')` affiche les plans : **Parsed Logical Plan**, **Analyzed Logical Plan**, **Optimized Logical Plan**, **Physical Plan**.

Plan **optimisé** de la version « filtre après la jointure » :

```text
Project [famille#7431, quantite#8654]
+- Join Inner, (id_produit#8652 = id_produit#7430)
   :- Project [id_produit#8652, quantite#8654]
   :  +- Filter ((isnotnull(pays#8653) AND (pays#8653 = FR)) AND isnotnull(id_produit#8652))
   :     +- Relation [id_commande#8651,id_produit#8652,pays#8653,quantite#8654,prix_unitaire#8655,date_commande#8656] parquet
   +- Filter isnotnull(id_produit#7430)
      +- Relation [id_produit#7430,famille#7431] parquet
```

Les plans optimisés des deux écritures sont **identiques** (une fois les identifiants internes `#123` retirés). Catalyst a **poussé le filtre** `pays = FR` sous la jointure, jusqu'au scan Parquet. Il a **élagué les colonnes**, en ne lisant que `id_produit`, `pays` et `quantite`. Il a aussi ajouté `isnotnull(id_produit)`, déduit de la jointure interne.

**Repli de constantes** : `id + lit(2) * lit(3)` devient, dans le plan optimisé :

```text
Project [(id#8658L + 6) AS v#8659L]
+- Range (0, 3, step=1, splits=Some(4))
```

## Q6. Mini-benchmark : CA par pays et famille

| Version | Ingrédients | Durée | Jobs | Shuffle write |
|---|---|---|---|---|
| Naïve | CSV + inferSchema, UDF Python, SortMergeJoin subie | 15.22 s | 4 | 69.2 Mo |
| Optimisée | Parquet, fonctions natives, broadcast, colonnes utiles | 0.95 s | 3 | 4.4 Ko |

**Gain : × 16**, pour le même résultat (écart maximal entre les deux CA : 0.000015).

Classement des optimisations par impact, dans l'ordre observé dans les exercices 3 à 5 :
1. **Format et schéma** : passer du CSV avec inférence au Parquet supprime une passe de lecture et le parsing, et divise le volume lu.
2. **Supprimer l'UDF Python** : c'est le facteur le plus lourd de l'exercice 3, car il évite la sérialisation de chaque ligne entre la JVM et Python.
3. **Broadcast** : il supprime le shuffle des 5 M faits et le skew sur P000 (exercice 4).
4. **Filtres et colonnes tôt** : Catalyst le fait souvent seul, mais il ne peut pas le faire à travers une UDF.
5. **Cache** : utile seulement quand un résultat est réutilisé.

