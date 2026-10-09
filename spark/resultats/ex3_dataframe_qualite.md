# Exercice 3 — DataFrame, qualité des données et SQL

_Résultats générés par l'exécution réelle du script (PySpark 4.2.0, local, N = 5 000 000 commandes). Les durées dépendent de la machine._

## Q1. `inferSchema` contre schéma explicite

**Prédiction** : l'inférence lance au moins un job de plus, car Spark relit le fichier pour deviner les types. `quantite` sera devinée en `string` à cause de `abc`, et `date_commande` aussi à cause de `2026-13-45`.

| Lecture | Jobs lancés à la lecture (sans action) |
|---|---|
| `inferSchema=True` | 2 |
| `schema(StructType)` | 0 |

| Colonne | Type inféré | Type attendu |
|---|---|---|
| id_commande | string | string |
| id_produit | string | string |
| pays | string | string |
| quantite | **string** | int |
| prix_unitaire | double | double |
| date_commande | **string** | date |

Le job supplémentaire est la passe d'inférence : Spark doit lire le fichier avant même que l'on demande un résultat. Il suffit d'**une seule valeur non conforme** pour qu'une colonne entière devienne `string` (`abc` dans `quantite`, `2026-13-45` dans les dates). Le schéma explicite évite cette passe et garantit des types stables d'un chargement à l'autre.

## Q2. Modes PERMISSIVE, DROPMALFORMED, FAILFAST

| Mode | Lignes obtenues | Remarque |
|---|---|---|
| PERMISSIVE | 200 003 | 3 lignes avec `_corrupt_record` non nul |
| DROPMALFORMED | 200 000 | lignes supprimées **sans avertissement** ; `count()` sans cache renvoie 200 003 car Spark ne parse alors aucune colonne |
| FAILFAST | — | **échec du job** (`Py4JJavaError` : malformed record) |

Lignes corrompues détectées :

```text
C_BAD1,P001,FR,abc,10.5,2026-02-01
C_BAD2,P002,BE
C_BAD3,P003,ES,2,12.0,2026-13-45
```

En production, il faut choisir **PERMISSIVE + table de rejets** : aucune ligne n'est perdue en silence, la ligne brute reste traçable, et le traitement continue. DROPMALFORMED masque les problèmes. FAILFAST convient à un contrôle d'entrée strict, où l'on préfère bloquer le chargement. Le `cache()` est nécessaire : sans lui, l'**élagage de colonnes** fait que Spark ne parse pas les champs non demandés et peut ne pas détecter la corruption.

## Q3. Contrôles qualité en un seul passage (5 M lignes)

| Contrôle | Valeur |
|---|---|
| null_id_commande | 0 |
| null_id_produit | 0 |
| null_pays | 50 098 |
| null_quantite | 0 |
| null_prix_unitaire | 0 |
| null_date_commande | 0 |
| doublons_id | 0 |
| quantite_le_0 | 24 528 |
| date_invalide | 0 |
| total | 5 000 000 |

Tous ces contrôles tiennent en **un seul `agg`**, exécuté en 6.29 s (3 jobs, 3 stages). Le `countDistinct` impose un shuffle, et l'AQE exécute chaque stage comme un job distinct, mais les données ne sont lues qu'une fois. Dix `count()` séparés relanceraient dix fois la génération ou la lecture complète. On retrouve les anomalies voulues : environ 1 % de pays NULL et 0,5 % de quantités à -1.

## Q4. Pipeline propre avec table de rejets

| Motif de rejet | Lignes |
|---|---|
| ligne_corrompue | 3 |
| pays_absent | 1916 |
| quantite_invalide | 984 |

| Total | Propres | Rejets | Doublons | Propres + rejets + doublons = total ? |
|---|---|---|---|---|
| 200 003 | 197 100 | 2903 | 0 | **oui** |

Les rejets et les lignes propres sont écrits dans deux dossiers Parquet distincts (`data/ex3_rejets`, `data/ex3_propres`). Chaque rejet garde son motif, et la réconciliation est exacte.

## Q5. Fonction native, UDF Python et pandas_udf (5 M lignes)

**Prédiction** : la version native est la plus rapide, la pandas_udf est 2 à 5 fois plus lente, et l'UDF classique est la plus lente, d'un facteur 10 ou plus.

| Variante | Durée | Opérateur Python dans le plan |
|---|---|---|
| F.when (natif) | 0.82 s | `aucun (JVM)` |
| UDF Python | 8.26 s | `BatchEvalPython` |
| pandas_udf (Arrow) | 9.41 s | `ArrowEvalPython` |

La version native est **10 fois plus rapide** que l'UDF Python : prédiction confirmée sur ce point. L'UDF classique **sérialise chaque ligne** de la JVM vers un processus Python, puis renvoie le résultat (`BatchEvalPython`). Elle est aussi **opaque pour Catalyst**, qui ne peut ni la simplifier ni pousser de filtre à travers. `F.when` reste dans la JVM, avec le code généré par Tungsten.

**Écart avec la prédiction** : la pandas_udf (9.41 s) n'est pas plus rapide que l'UDF classique (8.26 s) ici. La fonction appliquée est triviale (deux comparaisons) : le coût est dominé par le **transfert des 5 millions de valeurs** entre la JVM et Python, identique dans les deux cas, et par les workers Python, lents à démarrer sous Windows. Le gain d'Arrow (`ArrowEvalPython`, traitement par blocs et vectorisé) apparaît quand la fonction Python elle-même est coûteuse, ce qui n'est pas le cas ici. La règle reste : **natif d'abord**, puis pandas_udf, et l'UDF Python seulement en dernier recours.

## Q6. Fonctions de fenêtre

**(a) Top 3 des produits par pays (CA)**

| pays | id_produit | ca | rang |
|---|---|---|---|
| BE | P000 | 316 408 639.04 | 1 |
| BE | P572 | 271 926.88 | 2 |
| BE | P796 | 254 402.86 | 3 |
| DE | P000 | 317 612 297.87 | 1 |
| DE | P098 | 255 569.75 | 2 |
| DE | P925 | 255 402.17 | 3 |
| ES | P000 | 316 734 725.98 | 1 |
| ES | P955 | 257 422.97 | 2 |
| ES | P347 | 254 652.50 | 3 |
| FR | P000 | 316 368 743.16 | 1 |
| FR | P764 | 254 373.76 | 2 |
| FR | P677 | 253 389.96 | 3 |
| IT | P000 | 316 623 996.94 | 1 |
| IT | P627 | 249 280.13 | 2 |
| IT | P131 | 248 589.34 | 3 |

Le plan de (a) contient **2 Exchange** : un pour le `groupBy(pays, id_produit)` et un pour `partitionBy(pays)` de la fenêtre, suivi d'un tri local par CA décroissant. Ici P000 arrive en tête partout, car il concentre 60 % des commandes.

**(b) Moyenne mobile 7 jours et (c) variation par rapport à la veille** (FR, 8 premiers jours)

| date | ca_jour | ca_moy_7j | var_j_1 |
|---|---|---|---|
| 2026-01-01 | 1885723.38 | 1885723.38 | None |
| 2026-01-02 | 1917534.9 | 1901629.14 | 31811.52 |
| 2026-01-03 | 2005059.52 | 1936105.93 | 87524.62 |
| 2026-01-04 | 1949118.66 | 1939359.12 | -55940.86 |
| 2026-01-05 | 1989779.17 | 1949443.13 | 40660.51 |
| 2026-01-06 | 2006686.75 | 1958983.73 | 16907.58 |
| 2026-01-07 | 1915539.23 | 1952777.37 | -91147.52 |
| 2026-01-08 | 2008681.15 | 1970342.77 | 93141.92 |

| v | row_number | rank | dense_rank |
|---|---|---|---|
| 100 | 1 | 1 | 1 |
| 100 | 2 | 1 | 1 |
| 90 | 3 | 3 | 2 |

`row_number` numérote sans ex æquo, `rank` laisse un trou après les ex æquo (1, 1, 3), et `dense_rank` n'en laisse pas (1, 1, 2). Pour un « top 3 strict », `row_number` est le bon choix. Pour garder les ex æquo, on utilise `dense_rank`.

## Q7. Même KPI en Spark SQL

```sql
SELECT pays, id_produit, ca, rang FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY pays ORDER BY ca DESC) AS rang
  FROM ca_pays_produit) t
WHERE rang <= 3
```

`top.exceptAll(top_sql).count() = 0` et `top_sql.exceptAll(top).count() = 0` : les deux résultats sont **identiques**. Les plans physiques sont identiques dans leur structure (mêmes Exchange, Window, Filter), car DataFrame et SQL passent par le même optimiseur Catalyst.

## Q8. Pièges des NULL

| Expression | Prédiction | Résultat Spark |
|---|---|---|
| `NULL = NULL` | NULL | `NULL` |
| `count(pays) / count(*)` | 2 / 3 | `2 / 3` |
| `sum d'une colonne toute NULL` | NULL | `NULL` |
| `NULL <=> NULL` | true | `true` |
| `filter(pays = NULL)` | 0 | `0` |
| `filter(pays IS NULL)` | 1 | `1` |

NULL signifie « inconnu » : `NULL = NULL` est inconnu, donc NULL. Un `filter` ne garde que les lignes où la condition vaut **vrai**, et une condition à NULL élimine la ligne : `pays = NULL` ne renvoie jamais rien. Il faut écrire `isNull()` ou utiliser `<=>` (`eqNullSafe`). `count(col)` ignore les NULL, `count(*)` non.

