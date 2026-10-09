"""Exécute les six exercices Spark l'un après l'autre et régénère resultats/*.md."""
import importlib
import time

EXERCICES = ["ex1_execution", "ex2_rdd", "ex3_dataframe_qualite", "ex4_jointures_skew",
             "ex5_formats_optimiseur", "ex6_streaming"]

if __name__ == "__main__":
    for nom in EXERCICES:
        t0 = time.perf_counter()
        importlib.import_module(nom).main()
        print(f"{nom} terminé en {time.perf_counter() - t0:.0f} s")
