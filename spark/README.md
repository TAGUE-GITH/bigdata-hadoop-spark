# Spark — 6 exercices (PySpark 4.2.0)

Chaque exercice suit le cycle **prédire → exécuter → observer → expliquer**. Les prédictions sont
écrites dans le script **avant** le code mesuré. Les mesures (jobs, stages, tâches, shuffle, durée
des tâches) sont lues automatiquement dans l'API REST de la Spark UI (`common.mesurer`). Quand la
mesure contredit la prédiction, l'écart est expliqué au lieu d'être masqué.

| Exercice | Script | Compte rendu généré | Résultat marquant |
|---|---|---|---|
| 1. Anatomie d'une exécution | [ex1_execution.py](ex1_execution.py) | [ex1](resultats/ex1_execution.md) | AQE : 8 → 1 tâche de reduce ; 8 partitions optimales (0,85 s contre 1,7 s avec 2 et 1,4 s avec 64) |
| 2. RDD et variables partagées | [ex2_rdd.py](ex2_rdd.py) | [ex2](resultats/ex2_rdd.md) | accumulateur dans un `map` = **2 fois** la vraie valeur ; panne simulée rattrapée par `local[4,3]` |
| 3. DataFrame, qualité, SQL | [ex3_dataframe_qualite.py](ex3_dataframe_qualite.py) | [ex3](resultats/ex3_dataframe_qualite.md) | natif **10 fois** plus rapide qu'une UDF ; propres + rejets + doublons = total |
| 4. Jointures et skew | [ex4_jointures_skew.py](ex4_jointures_skew.py) | [ex4](resultats/ex4_jointures_skew.md) | skew mesuré (tâche max / médiane = **9,0**) puis corrigé par AQE, salage et clé chaude ; broadcast 7,5 fois plus rapide |
| 5. Formats, cache, Catalyst | [ex5_formats_optimiseur.py](ex5_formats_optimiseur.py) | [ex5](resultats/ex5_formats_optimiseur.md) | Parquet 4 fois plus petit que le CSV ; 8 000 → 1 000 fichiers ; benchmark naïf / optimisé **× 16** |
| 6. Structured Streaming | [ex6_streaming.py](ex6_streaming.py) | [ex6](resultats/ex6_streaming.md) | watermark : 43 événements à 10 min ignorés et état borné ; reprise sur checkpoint sans doublon ni trou |

## Exécuter

```bash
pip install -r ../requirements.txt
python run_all.py          # environ 12 min sur 8 cœurs ; ou un exercice : python ex4_jointures_skew.py
```

`common.py` génère le jeu de l'énoncé : 5 M commandes, dont 60 % sur P000, 1 % de pays absents et
0,5 % de quantités à −1. Il le matérialise une fois en Parquet dans `data/`. La taille se règle par
la variable `TP_N`, par exemple `TP_N=2000000` sur Colab.

## Écarts entre prédictions et mesures (documentés dans les comptes rendus)

- **Ex. 1** : sans AQE, `show()` lance 3 jobs, pas 1, car `collectLimit` lit 1, puis 4, puis 3
  partitions du shuffle.
- **Ex. 2** : `groupByKey` n'écrit « que » 4 fois plus au shuffle. PySpark regroupe déjà les
  valeurs en listes côté map, et la compression lz4 réduit fortement ces listes répétitives.
- **Ex. 3** : `pandas_udf` n'est pas plus rapide que l'UDF classique pour une fonction triviale, car
  le coût est dominé par le transfert entre la JVM et Python.
- **Ex. 6** : `numRowsDroppedByWatermark` compte des lignes pré-agrégées, pas des événements.

## Environnement

Exécuté sous **Windows 11** (Java 21, 8 cœurs, 24 Go) sans `winutils.exe`, grâce au petit
adaptateur Java [`tools/winfs-shim`](../tools/winfs-shim). Sur Linux ou Colab, il n'est pas utilisé.
Les durées sont indicatives : elles dépendent de la machine, et les workers Python sont plus lents
à démarrer sous Windows.
