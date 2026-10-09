"""
TP 1 Hadoop - Architecte HDFS : dimensionnement, placement, panne et re-réplication.

Calcule et simule ce que ferait le NameNode pour un fichier de 1 200 Mo
(blocs de 128 Mo, réplication 3, 6 DataNodes), puis écrit TP1_resultats.md.
"""
from __future__ import annotations

import math
from collections import Counter
from pathlib import Path

MO = 1024 * 1024
TAILLE_FICHIER_MO, BLOC_MO, REPLICATION = 1200, 128, 3
DATANODES = [f"DN{i}" for i in range(1, 7)]


def dimensionner(taille_mo: int, bloc_mo: int, replication: int) -> dict:
    nb_blocs = math.ceil(taille_mo / bloc_mo)
    dernier = taille_mo - (nb_blocs - 1) * bloc_mo
    return {"blocs": nb_blocs, "dernier_mo": dernier, "copies": nb_blocs * replication,
            "physique_mo": taille_mo * replication, "ratio": taille_mo / bloc_mo}


def placement_initial(nb_blocs: int) -> dict[str, list[str]]:
    """Glouton équilibré : chaque bloc va sur les 3 DataNodes les moins chargés (départage en rotation)."""
    charge_dn = Counter({dn: 0 for dn in DATANODES})
    placement = {}
    for i in range(nb_blocs):
        ordre = sorted(DATANODES, key=lambda dn: (charge_dn[dn], (DATANODES.index(dn) - i) % 6))
        placement[f"B{i + 1}"] = sorted(ordre[:REPLICATION], key=DATANODES.index)
        charge_dn.update(placement[f"B{i + 1}"])
    return placement


def charge(placement) -> Counter:
    return Counter(dn for copies in placement.values() for dn in copies)


def rereplication(placement, pannes: set[str]):
    """Pour chaque bloc touché, le NameNode choisit un DataNode actif ne possédant pas déjà le bloc, le moins chargé."""
    apres = {b: [dn for dn in copies if dn not in pannes] for b, copies in placement.items()}
    actions = []
    actifs = [dn for dn in DATANODES if dn not in pannes]
    for bloc, restantes in apres.items():
        while len(restantes) < REPLICATION and restantes:
            c = charge(apres)
            cible = min((dn for dn in actifs if dn not in restantes), key=lambda dn: (c[dn], dn))
            actions.append((bloc, list(restantes), restantes[0], cible))
            restantes.append(cible)
    return apres, actions


def tableau(entetes, lignes):
    out = ["| " + " | ".join(entetes) + " |", "|" + "|".join("---" for _ in entetes) + "|"]
    out += ["| " + " | ".join(str(x) for x in l) + " |" for l in lignes]
    return out + [""]


def main():
    d = dimensionner(TAILLE_FICHIER_MO, BLOC_MO, REPLICATION)
    place = placement_initial(d["blocs"])
    L = ["# TP 1 — Architecte HDFS : résultats calculés", "",
         "_Généré par `calculs_hdfs.py`._", "",
         "## Mission A — Dimensionnement (1 200 Mo, blocs de 128 Mo, réplication 3)", ""]
    L += tableau(["Grandeur", "Calcul", "Résultat"], [
        ["Blocs logiques", f"⌈1 200 / 128⌉ = ⌈{d['ratio']}⌉", f"**{d['blocs']} blocs**"],
        ["Taille du dernier bloc", f"1 200 − 9 × 128", f"**{d['dernier_mo']} Mo** ({d['dernier_mo'] * MO:,} octets)".replace(",", " ")],
        ["Copies physiques", f"{d['blocs']} × {REPLICATION}", f"**{d['copies']} copies**"],
        ["Volume physique occupé", f"1 200 Mo × {REPLICATION}", f"**{d['physique_mo']} Mo ≈ {d['physique_mo'] / 1024:.2f} Go**"],
    ])
    L += ["Le dernier bloc n'occupe que 48 Mo sur disque : HDFS ne réserve pas 128 Mo pour un bloc "
          "partiellement rempli. La taille de bloc est un **maximum logique**, pas une allocation. Le "
          "volume physique vaut donc exactement 3 × 1 200 Mo, hors métadonnées et sommes de contrôle "
          "(fichiers `.meta`, environ 0,8 %).", ""]

    L += ["## Mission B — Placement initial", "",
          "Règle retenue : chaque bloc va sur les **trois DataNodes les moins chargés** au moment de "
          "l'écriture, avec un départage en rotation pour varier les combinaisons. Les trois copies d'un "
          "bloc sont toujours sur trois nœuds différents.", ""]
    L += tableau(["Bloc", "Taille", "Copie 1", "Copie 2", "Copie 3"],
                 [[b, f"{BLOC_MO if i < d['blocs'] - 1 else d['dernier_mo']} Mo", *c] for i, (b, c) in enumerate(place.items())])
    c = charge(place)
    L += tableau(["DataNode", *DATANODES], [["Copies hébergées", *[c[dn] for dn in DATANODES]]])
    L += ["La répartition est équilibrée : 30 copies pour 6 nœuds, soit 5 chacun. Aucun bloc n'a deux "
          "copies sur le même nœud, donc la perte d'un DataNode laisse toujours au moins 2 copies. "
          "_Sur un vrai cluster, la politique par défaut tient aussi compte des racks : 1re copie sur le "
          "nœud écrivain, 2e et 3e sur un autre rack._", ""]

    apres, actions = rereplication(place, {"DN3"})
    touches = [b for b, copies in place.items() if "DN3" in copies]
    L += ["## Mission C — Panne de DN3", "",
          f"**Blocs sous-répliqués** ({len(touches)}) : {', '.join(touches)}.", ""]
    L += tableau(["Bloc", "Copies encore disponibles", "Source de la copie", "Nouvelle copie sur"],
                 [[b, " + ".join(rest), src, f"**{cible}**"] for b, rest, src, cible in actions])
    L += ["Après re-réplication :", ""]
    L += tableau(["Bloc", "Copie 1", "Copie 2", "Copie 3"], [[b, *cps] for b, cps in apres.items()])
    c2 = charge(apres)
    L += tableau(["DataNode", *[dn for dn in DATANODES if dn != "DN3"]],
                 [["Copies hébergées", *[c2[dn] for dn in DATANODES if dn != "DN3"]]])
    L += ["**Rôle des composants :**",
          "- **Heartbeats** : chaque DataNode signale qu'il est vivant toutes les 3 s. Sans heartbeat "
          "pendant environ 10 min 30 s (`2 × recheck-interval + 10 × heartbeat`), le NameNode déclare DN3 mort.",
          "- **NameNode** : il ne stocke aucune donnée. Il tient la carte bloc → DataNodes, reconstruite à "
          "partir des *block reports*. Il détecte les blocs sous-répliqués, choisit une source et une "
          "cible, et place les ordres de copie dans la réponse au heartbeat du DataNode source.",
          "- **Transferts** : la copie se fait **directement de DataNode à DataNode**, sans passer par le "
          "NameNode. Le fichier reste lisible pendant toute l'opération, puisque 2 copies existent toujours.", ""]

    pd2 = dimensionner(TAILLE_FICHIER_MO, BLOC_MO, 2)
    double, actions2 = rereplication(place, {"DN3", "DN4"})
    deux_pertes = [b for b, cps in place.items() if {"DN3", "DN4"} <= set(cps)]
    perdus = [b for b, cps in place.items() if set(cps) <= {"DN3", "DN4"}]
    L += ["## Bonus", "",
          f"**Réplication 2** : {pd2['copies']} copies, soit {pd2['physique_mo']} Mo au lieu de "
          f"{d['physique_mo']} Mo, une économie de 1 200 Mo (−33 %). En contrepartie, la perte d'un seul "
          "nœud laisse des blocs avec une **copie unique**. Une seconde panne avant la fin de la "
          "re-réplication rendrait ces blocs illisibles.", "",
          "**Compromis** : chaque copie supplémentaire coûte 100 % du volume utile, mais elle protège "
          "contre une panne de plus et offre plus de nœuds pour lire en parallèle. La réplication 3 est "
          "le standard pour les données de production. On peut descendre à 2, ou passer à l'*erasure "
          "coding* (environ 1,5×), pour des données froides ou reconstructibles.", "",
          f"**DN3 et DN4 en panne simultanément** : {len(deux_pertes)} blocs ({', '.join(deux_pertes)}) "
          f"perdent 2 copies sur 3 et n'en gardent qu'**une**. Aucun bloc n'est perdu "
          f"({len(perdus)} bloc sans copie restante), car aucun n'avait ses 3 copies sur ces deux nœuds. "
          "Le NameNode traite **en priorité** les blocs à copie unique, puis les autres. "
          f"{len(actions2)} nouvelles copies sont nécessaires.", ""]

    L += ["## Partie Colab — fichier de 300 Mo (pseudo-distribué, 1 DataNode)", ""]
    c3 = dimensionner(300, BLOC_MO, 3)
    L += tableau(["Bloc", "Longueur attendue dans `fsck`"],
                 [["blk 1", f"{BLOC_MO * MO:,} octets (128 Mo)".replace(",", " ")],
                  ["blk 2", f"{BLOC_MO * MO:,} octets (128 Mo)".replace(",", " ")],
                  ["blk 3", f"{c3['dernier_mo'] * MO:,} octets ({c3['dernier_mo']} Mo)".replace(",", " ")]])
    L += [f"1. 300 / 128 = {c3['ratio']} : il faut **3 blocs**, deux pleins et un de {c3['dernier_mo']} Mo.",
          "2. Les longueurs ci-dessus sont celles affichées par `hdfs fsck -files -blocks -locations` : "
          "`len=134217728`, `len=134217728`, `len=46137344`.",
          "3. **Réplication demandée ou réelle** : `-Ddfs.replication=3` enregistre un facteur 3 dans les "
          "métadonnées du fichier (`hdfs dfs -stat %r` → 3). Avec un seul DataNode, une seule copie peut "
          "exister (`Live_repl=1`) : `fsck` signale le fichier **Under replicated**. Il reste lisible, "
          "mais sans aucune tolérance aux pannes.",
          "4. **Ne pas arrêter le seul DataNode** : il porte l'unique copie de chaque bloc. Les blocs "
          "deviennent *missing*, les lectures échouent, et au redémarrage le NameNode peut rester en "
          "**safe mode** tant que les blocs ne sont pas signalés.",
          "5. **300 Mo pratique contre 1 200 Mo théorique** : même règle de découpage, ⌈taille / 128⌉ blocs et "
          "un dernier bloc partiel (44 Mo et 48 Mo). Le facteur 3 n'est réellement appliqué qu'avec au "
          "moins 3 DataNodes : en Colab, on observe le découpage, pas la réplication.", ""]
    L += ["## Conclusion (10 lignes)", "",
          "Un fichier de 1 200 Mo occupe 10 blocs logiques (9 × 128 Mo + 48 Mo), soit 30 copies et "
          "3 600 Mo physiques avec la réplication 3. Un placement équilibré sur 6 DataNodes répartit "
          "5 copies par nœud et garantit qu'aucun bloc n'a deux copies sur le même nœud. La perte de DN3 "
          f"rend {len(touches)} blocs sous-répliqués mais aucun indisponible. Le NameNode, alerté par l'absence de "
          f"heartbeats, ordonne {len(actions)} copies directes entre DataNodes vers les nœuds les moins chargés. "
          "Colab ne permet que la **pseudo-distribution** : on y vérifie le découpage en blocs et la "
          "sous-réplication, mais pas la tolérance aux pannes, qui exige une **distribution réelle** sur "
          "plusieurs machines (exercice Docker à 6 DataNodes). Le calcul théorique est une **simulation** : "
          "il prédit ce que le cluster réel doit montrer.", ""]
    Path(__file__).with_name("TP1_resultats.md").write_text("\n".join(L), encoding="utf-8")
    print("\n".join(L))


if __name__ == "__main__":
    main()
