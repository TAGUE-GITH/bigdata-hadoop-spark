"""Tests du Mapper et du Reducer (python -m pytest ou python test_mapreduce.py)."""
import subprocess
import sys
from pathlib import Path

ICI = Path(__file__).parent


def executer(script, entree):
    r = subprocess.run([sys.executable, str(ICI / script)], input=entree, capture_output=True, text=True, check=True)
    return r.stdout, r.stderr


def compteurs(stderr):
    c = {}
    for l in stderr.splitlines():
        _, _, groupe_nom_val = l.partition("reporter:counter:")
        _, nom, val = groupe_nom_val.split(",")
        c[nom] = c.get(nom, 0) + int(val)
    return c


def test_entete_et_lignes_invalides():
    out, err = executer("mapper.py", "date;pays;produit;quantite;prix_unitaire\n"
                        "2026-10-04;DE;P04;abc;20.00\n2026-10-04;;P05;2;9.50\n2026-10-05;FR;P06;-1;30.00\n"
                        "2026-10-05;FR;P06;2;-3\n2026-10-05;FR;P06;2;x\n2026-10-05;FR\n\n")
    assert out == ""
    c = compteurs(err)
    assert c == {"Entete_ignoree": 1, "Lignes_lues": 6, "Quantite_invalide": 1, "Pays_manquant": 1,
                 "Valeur_negative": 2, "Prix_invalide": 1, "Format_invalide": 1}


def test_ligne_valide_crlf():
    out, _ = executer("mapper.py", "2026-10-01;FR;P01;3;0.10\r\n")
    assert out.strip() == "FR\t0.30"


def test_reducer_sans_liste_de_pays():
    out, err = executer("reducer.py", "BE\t15.00\nBE\t40.00\nZZ\t0.005\nZZ\t0.005\n")
    assert out.split("\n")[:2] == ["BE\t55.00", "ZZ\t0.01"]
    assert compteurs(err) == {"Groupes_reduce": 2}


def test_bout_en_bout():
    mapped, _ = executer("mapper.py", (ICI / "ventes.csv").read_text())
    out, _ = executer("reducer.py", "".join(sorted(mapped.splitlines(keepends=True))))
    assert out.split() == ["BE", "55.00", "ES", "30.00", "FR", "66.00"]


if __name__ == "__main__":
    for nom, f in list(globals().items()):
        if nom.startswith("test_"):
            f()
            print("OK", nom)
