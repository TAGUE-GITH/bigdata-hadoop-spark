#!/usr/bin/env python3
"""Reducer Hadoop Streaming : somme des montants par pays.

Les entrées arrivent triées par clé (Shuffle/Sort) : on cumule tant que la clé
ne change pas. Aucune liste de pays n'est codée en dur.
"""
import sys
from decimal import ROUND_HALF_UP, Decimal


def emettre(pays, total):
    print(f"{pays}\t{total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}")
    sys.stderr.write("reporter:counter:Qualite,Groupes_reduce,1\n")


def main():
    pays_courant, total = None, Decimal(0)
    for ligne in sys.stdin:
        ligne = ligne.rstrip("\r\n")
        if not ligne:
            continue
        pays, montant = ligne.split("\t", 1)
        if pays != pays_courant:
            if pays_courant is not None:
                emettre(pays_courant, total)
            pays_courant, total = pays, Decimal(0)
        total += Decimal(montant)
    if pays_courant is not None:
        emettre(pays_courant, total)


if __name__ == "__main__":
    main()
