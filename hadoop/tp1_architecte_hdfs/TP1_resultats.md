# TP 1 — Architecte HDFS : résultats calculés

_Généré par `calculs_hdfs.py`._

## Mission A — Dimensionnement (1 200 Mo, blocs de 128 Mo, réplication 3)

| Grandeur | Calcul | Résultat |
|---|---|---|
| Blocs logiques | ⌈1 200 / 128⌉ = ⌈9.375⌉ | **10 blocs** |
| Taille du dernier bloc | 1 200 − 9 × 128 | **48 Mo** (50 331 648 octets) |
| Copies physiques | 10 × 3 | **30 copies** |
| Volume physique occupé | 1 200 Mo × 3 | **3600 Mo ≈ 3.52 Go** |

Le dernier bloc n'occupe que 48 Mo sur disque : HDFS ne réserve pas 128 Mo pour un bloc partiellement rempli. La taille de bloc est un **maximum logique**, pas une allocation. Le volume physique vaut donc exactement 3 × 1 200 Mo, hors métadonnées et sommes de contrôle (fichiers `.meta`, environ 0,8 %).

## Mission B — Placement initial

Règle retenue : chaque bloc va sur les **trois DataNodes les moins chargés** au moment de l'écriture, avec un départage en rotation pour varier les combinaisons. Les trois copies d'un bloc sont toujours sur trois nœuds différents.

| Bloc | Taille | Copie 1 | Copie 2 | Copie 3 |
|---|---|---|---|---|
| B1 | 128 Mo | DN1 | DN2 | DN3 |
| B2 | 128 Mo | DN4 | DN5 | DN6 |
| B3 | 128 Mo | DN3 | DN4 | DN5 |
| B4 | 128 Mo | DN1 | DN2 | DN6 |
| B5 | 128 Mo | DN1 | DN5 | DN6 |
| B6 | 128 Mo | DN2 | DN3 | DN4 |
| B7 | 128 Mo | DN1 | DN2 | DN3 |
| B8 | 128 Mo | DN4 | DN5 | DN6 |
| B9 | 128 Mo | DN3 | DN4 | DN5 |
| B10 | 48 Mo | DN1 | DN2 | DN6 |

| DataNode | DN1 | DN2 | DN3 | DN4 | DN5 | DN6 |
|---|---|---|---|---|---|---|
| Copies hébergées | 5 | 5 | 5 | 5 | 5 | 5 |

La répartition est équilibrée : 30 copies pour 6 nœuds, soit 5 chacun. Aucun bloc n'a deux copies sur le même nœud, donc la perte d'un DataNode laisse toujours au moins 2 copies. _Sur un vrai cluster, la politique par défaut tient aussi compte des racks : 1re copie sur le nœud écrivain, 2e et 3e sur un autre rack._

## Mission C — Panne de DN3

**Blocs sous-répliqués** (5) : B1, B3, B6, B7, B9.

| Bloc | Copies encore disponibles | Source de la copie | Nouvelle copie sur |
|---|---|---|---|
| B1 | DN1 + DN2 | DN1 | **DN4** |
| B3 | DN4 + DN5 | DN4 | **DN1** |
| B6 | DN2 + DN4 | DN2 | **DN5** |
| B7 | DN1 + DN2 | DN1 | **DN6** |
| B9 | DN4 + DN5 | DN4 | **DN2** |

Après re-réplication :

| Bloc | Copie 1 | Copie 2 | Copie 3 |
|---|---|---|---|
| B1 | DN1 | DN2 | DN4 |
| B2 | DN4 | DN5 | DN6 |
| B3 | DN4 | DN5 | DN1 |
| B4 | DN1 | DN2 | DN6 |
| B5 | DN1 | DN5 | DN6 |
| B6 | DN2 | DN4 | DN5 |
| B7 | DN1 | DN2 | DN6 |
| B8 | DN4 | DN5 | DN6 |
| B9 | DN4 | DN5 | DN2 |
| B10 | DN1 | DN2 | DN6 |

| DataNode | DN1 | DN2 | DN4 | DN5 | DN6 |
|---|---|---|---|---|---|
| Copies hébergées | 6 | 6 | 6 | 6 | 6 |

**Rôle des composants :**
- **Heartbeats** : chaque DataNode signale qu'il est vivant toutes les 3 s. Sans heartbeat pendant environ 10 min 30 s (`2 × recheck-interval + 10 × heartbeat`), le NameNode déclare DN3 mort.
- **NameNode** : il ne stocke aucune donnée. Il tient la carte bloc → DataNodes, reconstruite à partir des *block reports*. Il détecte les blocs sous-répliqués, choisit une source et une cible, et place les ordres de copie dans la réponse au heartbeat du DataNode source.
- **Transferts** : la copie se fait **directement de DataNode à DataNode**, sans passer par le NameNode. Le fichier reste lisible pendant toute l'opération, puisque 2 copies existent toujours.

## Bonus

**Réplication 2** : 20 copies, soit 2400 Mo au lieu de 3600 Mo, une économie de 1 200 Mo (−33 %). En contrepartie, la perte d'un seul nœud laisse des blocs avec une **copie unique**. Une seconde panne avant la fin de la re-réplication rendrait ces blocs illisibles.

**Compromis** : chaque copie supplémentaire coûte 100 % du volume utile, mais elle protège contre une panne de plus et offre plus de nœuds pour lire en parallèle. La réplication 3 est le standard pour les données de production. On peut descendre à 2, ou passer à l'*erasure coding* (environ 1,5×), pour des données froides ou reconstructibles.

**DN3 et DN4 en panne simultanément** : 3 blocs (B3, B6, B9) perdent 2 copies sur 3 et n'en gardent qu'**une**. Aucun bloc n'est perdu (0 bloc sans copie restante), car aucun n'avait ses 3 copies sur ces deux nœuds. Le NameNode traite **en priorité** les blocs à copie unique, puis les autres. 10 nouvelles copies sont nécessaires.

## Partie Colab — fichier de 300 Mo (pseudo-distribué, 1 DataNode)

| Bloc | Longueur attendue dans `fsck` |
|---|---|
| blk 1 | 134 217 728 octets (128 Mo) |
| blk 2 | 134 217 728 octets (128 Mo) |
| blk 3 | 46 137 344 octets (44 Mo) |

1. 300 / 128 = 2.34375 : il faut **3 blocs**, deux pleins et un de 44 Mo.
2. Les longueurs ci-dessus sont celles affichées par `hdfs fsck -files -blocks -locations` : `len=134217728`, `len=134217728`, `len=46137344`.
3. **Réplication demandée ou réelle** : `-Ddfs.replication=3` enregistre un facteur 3 dans les métadonnées du fichier (`hdfs dfs -stat %r` → 3). Avec un seul DataNode, une seule copie peut exister (`Live_repl=1`) : `fsck` signale le fichier **Under replicated**. Il reste lisible, mais sans aucune tolérance aux pannes.
4. **Ne pas arrêter le seul DataNode** : il porte l'unique copie de chaque bloc. Les blocs deviennent *missing*, les lectures échouent, et au redémarrage le NameNode peut rester en **safe mode** tant que les blocs ne sont pas signalés.
5. **300 Mo pratique contre 1 200 Mo théorique** : même règle de découpage, ⌈taille / 128⌉ blocs et un dernier bloc partiel (44 Mo et 48 Mo). Le facteur 3 n'est réellement appliqué qu'avec au moins 3 DataNodes : en Colab, on observe le découpage, pas la réplication.

## Conclusion (10 lignes)

Un fichier de 1 200 Mo occupe 10 blocs logiques (9 × 128 Mo + 48 Mo), soit 30 copies et 3 600 Mo physiques avec la réplication 3. Un placement équilibré sur 6 DataNodes répartit 5 copies par nœud et garantit qu'aucun bloc n'a deux copies sur le même nœud. La perte de DN3 rend 5 blocs sous-répliqués mais aucun indisponible. Le NameNode, alerté par l'absence de heartbeats, ordonne 5 copies directes entre DataNodes vers les nœuds les moins chargés. Colab ne permet que la **pseudo-distribution** : on y vérifie le découpage en blocs et la sous-réplication, mais pas la tolérance aux pannes, qui exige une **distribution réelle** sur plusieurs machines (exercice Docker à 6 DataNodes). Le calcul théorique est une **simulation** : il prédit ce que le cluster réel doit montrer.
