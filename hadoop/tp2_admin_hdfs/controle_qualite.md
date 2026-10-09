# TP 2 — Contrôle qualité des fichiers

_Généré par `controle_qualite.py`._

| Fichier | Lignes | Anomalie attendue | Constat |
|---|---:|---|---|
| ventes.csv | 41 | 1 ligne en double | 1 doublon exact : `2026-09-11, Allemagne, P001, 2, 13.38` (lignes 11, 12) |
| clients.csv | 30 | 1 pays manquant | client `C017` sans pays |
| produits.csv | 12 | 1 prix invalide | `P007` (Eclairage) à -24.0 € |
| logs_web.csv | 200 | plusieurs statuts 404 | 200 : 146, 301 : 5, 404 : 48, 500 : 1 |

Pages en erreur 404 : `/ancienne-page` (24), `/produits/xyz` (24). Il y a aussi 1 erreur 500 sur `/connexion` et 5 redirections 301.

Ces fichiers sont chargés **tels quels** dans la zone `raw`, car on ne corrige jamais la source. Les anomalies sont traitées à l'étape `curated`, où les lignes fautives partent dans `rejects`.
