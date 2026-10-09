"""TP 2 Hadoop - contrôle local des quatre fichiers avant chargement dans HDFS (anomalies attendues)."""
from pathlib import Path

import pandas as pd

DATA = Path(__file__).with_name("data")
ventes = pd.read_csv(DATA / "ventes.csv")
clients = pd.read_csv(DATA / "clients.csv")
produits = pd.read_csv(DATA / "produits.csv")
logs = pd.read_csv(DATA / "logs_web.csv")

lignes = ["# TP 2 — Contrôle qualité des fichiers", "", "_Généré par `controle_qualite.py`._", "",
          "| Fichier | Lignes | Anomalie attendue | Constat |", "|---|---:|---|---|"]
dup = ventes[ventes.duplicated(keep=False)]
lignes.append(f"| ventes.csv | {len(ventes)} | 1 ligne en double | {len(dup) // 2} doublon exact : "
              f"`{', '.join(map(str, dup.iloc[0].tolist()))}` (lignes {', '.join(str(i + 2) for i in dup.index)}) |")
manq = clients[clients["pays"].isna()]
lignes.append(f"| clients.csv | {len(clients)} | 1 pays manquant | client `{manq.iloc[0]['client_id']}` sans pays |")
inval = produits[produits["prix_ref"] <= 0]
lignes.append(f"| produits.csv | {len(produits)} | 1 prix invalide | `{inval.iloc[0]['produit_id']}` ({inval.iloc[0]['famille']}) "
              f"à {inval.iloc[0]['prix_ref']} € |")
statuts = logs["statut"].value_counts().sort_index()
lignes.append(f"| logs_web.csv | {len(logs)} | plusieurs statuts 404 | "
              + ", ".join(f"{s} : {n}" for s, n in statuts.items()) + " |")
pages_404 = logs[logs["statut"] == 404]["page"].value_counts()
lignes += ["", "Pages en erreur 404 : " + ", ".join(f"`{p}` ({n})" for p, n in pages_404.items()) +
           f". Il y a aussi {statuts.get(500, 0)} erreur 500 sur `/connexion` et "
           f"{statuts.get(301, 0)} redirections 301.", "",
           "Ces fichiers sont chargés **tels quels** dans la zone `raw`, car on ne corrige jamais la "
           "source. Les anomalies sont traitées à l'étape `curated`, où les lignes fautives partent dans "
           "`rejects`."]
Path(__file__).with_name("controle_qualite.md").write_text("\n".join(lignes) + "\n", encoding="utf-8")
print("\n".join(lignes))
