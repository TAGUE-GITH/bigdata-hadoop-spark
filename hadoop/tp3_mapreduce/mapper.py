#!/usr/bin/env python3
"""Mapper Hadoop Streaming : émet « pays<TAB>montant » pour chaque vente valide.

Les lignes invalides ne produisent aucune sortie : elles incrémentent un compteur
métier du groupe « Qualite » (protocole reporter:counter sur stderr).
"""
import sys
from decimal import Decimal, InvalidOperation

NB_COLONNES = 5


def compteur(nom, valeur=1):
    sys.stderr.write(f"reporter:counter:Qualite,{nom},{valeur}\n")


def main():
    for brute in sys.stdin:
        ligne = brute.strip()
        if not ligne:
            continue
        champs = [c.strip() for c in ligne.split(";")]
        if champs[0] == "date":                      # en-tête : aucune sortie
            compteur("Entete_ignoree")
            continue
        compteur("Lignes_lues")
        if len(champs) != NB_COLONNES:
            compteur("Format_invalide")
            continue
        _, pays, _, quantite, prix = champs
        if not pays:
            compteur("Pays_manquant")
            continue
        try:
            q = int(quantite)
        except ValueError:
            compteur("Quantite_invalide")
            continue
        try:
            p = Decimal(prix)
        except InvalidOperation:
            compteur("Prix_invalide")
            continue
        if q <= 0 or p <= 0:
            compteur("Valeur_negative")
            continue
        compteur("Lignes_valides")
        print(f"{pays}\t{q * p:.2f}")


if __name__ == "__main__":
    main()
