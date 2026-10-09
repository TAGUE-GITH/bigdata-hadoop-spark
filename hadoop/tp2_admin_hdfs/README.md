# TP 2 — Administrateur HDFS : organiser et contrôler un espace de données

| Fichier | Rôle |
|---|---|
| [`data/`](data) | Les 4 jeux de données fournis : ventes, clients, produits, logs web |
| [`controle_qualite.py`](controle_qualite.py) → [`controle_qualite.md`](controle_qualite.md) | Vérifie que les anomalies attendues sont présentes, exécuté en local |
| [`tp2_hdfs.sh`](tp2_hdfs.sh) | Toutes les commandes des missions A à D, avec les preuves écrites dans `preuves/` |

## Mission A — HDFS ou YARN

`jps` doit montrer **NameNode**, **DataNode** et **SecondaryNameNode**. Pour YARN, il doit montrer
**ResourceManager** et **NodeManager**. `hdfs dfsadmin -report` indique `Live datanodes (1)` sur Colab
et `(6)` sur le cluster Docker.

| | HDFS | YARN |
|---|---|---|
| Rôle | **Stocker** : découpe les fichiers en blocs répliqués | **Calculer** : attribue CPU et mémoire aux applications |
| Maître | NameNode (métadonnées : arborescence, blocs, emplacements) | ResourceManager (ordonnance les applications) |
| Esclaves | DataNodes (stockent les blocs) | NodeManagers (lancent les conteneurs) |
| Question traitée | *Où sont mes données ?* | *Où et avec quelles ressources lancer mon calcul ?* |

Les deux sont indépendants : Spark sur YARN peut lire S3, et HDFS peut servir Spark en mode standalone.
Leur colocalisation sur les mêmes machines permet la **localité des données**.

## Mission B — Zones

```
/data/
  raw/        données brutes, immuables, telles que reçues      ← ingestion_date=AAAA-MM-JJ
  curated/    données nettoyées, typées, dédoublonnées          ← year=/month= (partitions métier)
  analytics/  agrégats et indicateurs prêts à consommer          ← _READY quand le lot est complet
  rejects/    lignes écartées avec leur motif                    ← traçabilité des anomalies
```

- **raw** : copie fidèle de la source. On ne la modifie jamais, ce qui permet de **rejouer** tout le
  pipeline si une règle de nettoyage change.
- **curated** : la version de référence, fiable. Elle est partitionnée selon les filtres métier
  (année, mois), ce qui permet d'élaguer les partitions à la lecture.
- **analytics** : les tables d'usage (CA par pays, KPI…). Elles sont lues par les outils de BI.
- **rejects** : rien ne disparaît en silence. Chaque ligne exclue y est conservée avec sa raison.

**`ingestion_date`** identifie le **lot de chargement**, et non la date métier des données. On peut
ainsi recharger ou rejouer un lot précis, supprimer un lot défectueux sans toucher aux autres, et
appliquer une règle de rétention par date de réception.

## Mission C — Local ou HDFS

| Fichier | Taille | Blocs HDFS | Réplication |
|---|---:|---:|---:|
| ventes.csv | 1 396 o | 1 | 1 sur Colab, 3 sur cluster |
| clients.csv | 750 o | 1 | idem |
| produits.csv | 268 o | 1 | idem |
| logs_web.csv | 9 766 o | 1 | idem |

- **Chemin local** : `/content/.../data/ventes.csv`, sur le disque de la machine, visible par elle seule.
- **Chemin HDFS** : `/data/raw/sales/ingestion_date=2026-10-11/ventes.csv`, un espace de noms global
  géré par le NameNode et visible depuis tous les nœuds.
- La taille logique est identique des deux côtés (`stat %b`). Chaque fichier, plus petit que 128 Mo,
  occupe **un seul bloc**, dont la taille réelle sur disque est celle du fichier.
- `fsck` montre le bloc, sa longueur et le ou les DataNodes qui l'hébergent.

## Mission D — Droits et cycle de vie

| Droits | Propriétaire | Groupe | Autres | Usage |
|---|---|---|---|---|
| `750` sur `/data/curated` | rwx | r-x | --- | l'équipe data écrit ; les analystes du groupe lisent ; personne d'autre |
| `770` sur `/data/analytics` | rwx | rwx | --- | le propriétaire et le groupe lisent **et écrivent** (zone collaborative) |

Le bit `x` sur un répertoire HDFS autorise à le traverser et à lister son contenu.

**Marqueur `_READY`** : un fichier vide déposé **après** l'écriture complète du lot. Les consommateurs
(job, BI, ordonnanceur) attendent sa présence avant de lire, ce qui évite de lire un dossier à moitié
écrit. Spark utilise le même principe avec `_SUCCESS`.

**Règle de cycle de vie proposée pour `raw`** :

- garder 90 jours en ligne, soit l'horizon de rejeu ;
- au-delà, archiver en compressant, ou déplacer vers une zone ou un stockage froid (*storage policy* `COLD`) ;
- supprimer après 1 an, sauf contrainte légale ;
- la suppression ne porte que sur les dossiers `ingestion_date=` dont le lot `curated` correspondant porte `_READY`.
