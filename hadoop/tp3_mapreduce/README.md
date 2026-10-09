# TP 3 — MapReduce avancé : chiffre d'affaires et qualité des données

| Fichier | Rôle |
|---|---|
| [`mapper.py`](mapper.py) | Valide chaque ligne, émet `pays<TAB>montant` ou incrémente un compteur `Qualite` |
| [`reducer.py`](reducer.py) | Additionne les montants par pays (calcul en `Decimal`, arrondi à 2 décimales) |
| [`test_local.sh`](test_local.sh) | Mission C : `cat \| mapper \| sort \| reducer`, sans Hadoop |
| [`test_mapreduce.py`](test_mapreduce.py) | Tests automatiques : en-tête, lignes invalides, CRLF, reducer générique, bout en bout |
| [`run_hadoop.sh`](run_hadoop.sh) | Mission D : job Hadoop Streaming réel |

## Règles de qualité

| Situation | Compteur | Sortie |
|---|---|---|
| En-tête `date;pays;...` | `Entete_ignoree` | aucune |
| Nombre de colonnes différent de 5 | `Format_invalide` | aucune |
| Pays vide | `Pays_manquant` | aucune |
| Quantité non entière (`abc`) | `Quantite_invalide` | aucune |
| Prix non numérique | `Prix_invalide` | aucune |
| Quantité ≤ 0 ou prix ≤ 0 | `Valeur_negative` | aucune |
| Ligne valide | `Lignes_valides` | `pays<TAB>quantité × prix` |

Les compteurs sont émis sur **stderr** au format `reporter:counter:Qualite,<Nom>,1`. Hadoop
Streaming les agrège et les affiche à la fin du job.

## Mission C — Test local (exécuté)

```text
=== Sortie du Mapper ===
FR	20.00
BE	15.00
FR	36.00
BE	40.00
ES	30.00
FR	10.00
=== Compteurs du Mapper ===
      1 reporter:counter:Qualite,Entete_ignoree,1
      9 reporter:counter:Qualite,Lignes_lues,1
      6 reporter:counter:Qualite,Lignes_valides,1
      1 reporter:counter:Qualite,Pays_manquant,1
      1 reporter:counter:Qualite,Quantite_invalide,1
      1 reporter:counter:Qualite,Valeur_negative,1
=== Résultat final (après sort + reducer) ===
BE	55.00
ES	30.00
FR	66.00
```

**Vérification manuelle** :

- FR = 2×10 + 3×12 + 1×10 = **66,00**
- BE = 1×15 + 4×10 = **55,00**
- ES = 2×15 = **30,00**
- Lignes rejetées : DE (`abc`), la ligne sans pays, et FR (`-1`).

**Pourquoi `sort` simule le Shuffle/Sort** : le mapper émet les paires dans l'ordre de lecture
(FR, BE, FR…). Hadoop les **partitionne par clé** puis les **trie** avant le reducer, qui reçoit
toutes les valeurs d'une même clé **à la suite**. `sort` reproduit ce regroupement sur une seule
machine. Le reducer peut alors cumuler tant que la clé ne change pas, sans garder en mémoire la
liste des pays. C'est pour cela qu'aucun pays n'est codé en dur.

## Mission E — Analyse des compteurs Hadoop

| Compteur | Valeur attendue | Signification |
|---|---:|---|
| Map input records | **10** | toutes les lignes lues, en-tête compris |
| Map output records | **6** | lignes valides émises |
| Reduce input groups | **3** | clés distinctes après le shuffle : BE, ES, FR |
| Reduce output records | **3** | une ligne de résultat par pays |
| Qualite.Lignes_valides | 6 | |
| Qualite.Pays_manquant / Quantite_invalide / Valeur_negative | 1 / 1 / 1 | |

Réconciliation : **10 lues = 1 en-tête + 6 valides + 3 rejetées**. Les 6 lignes valides forment
**3 groupes Reduce**. Le nombre de groupes dépend du nombre de clés distinctes, pas du nombre de lignes.

**Erreur provoquée (Q22)** : relancer le job sans supprimer la sortie donne
`org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://localhost:9000/tp_avance/ca/output already exists`.
Hadoop refuse d'écraser un résultat existant, ce qui protège contre la perte silencieuse d'un
résultat précédent. Correction : `hdfs dfs -rm -r -f /tp_avance/ca/output` avant le job (ligne
présente dans `run_hadoop.sh`), ou un chemin de sortie horodaté.

> Le job Hadoop réel (`run_hadoop.sh`) se lance sur Colab ou sur un cluster. Les compteurs ci-dessus
> sont les valeurs logiques attendues, cohérentes avec le test local exécuté.
