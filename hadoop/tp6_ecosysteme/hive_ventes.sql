-- TP 6 Hadoop - Mission E : Hive sur les ventes stockées dans HDFS.

-- Table EXTERNE : Hive ne gère que le schéma. Un DROP TABLE ne supprime pas les fichiers HDFS.
CREATE EXTERNAL TABLE IF NOT EXISTS ventes_raw (
    date_vente  DATE,
    pays        STRING,
    produit     STRING,
    quantite    INT,
    prix        DECIMAL(10,2)
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///data/raw/sales'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Déclare les dossiers ingestion_date=... déjà présents dans HDFS
MSCK REPAIR TABLE ventes_raw;

-- Chiffre d'affaires par pays, sans doublons ni quantités invalides
SELECT pays,
       ROUND(SUM(quantite * prix), 2) AS ca,
       COUNT(*)                       AS nb_ventes
FROM (SELECT DISTINCT date_vente, pays, produit, quantite, prix FROM ventes_raw) v
WHERE quantite > 0 AND pays IS NOT NULL
GROUP BY pays
ORDER BY ca DESC;

-- Variante mensuelle (besoin n°37 : CA mensuel par pays)
SELECT pays, date_format(date_vente, 'yyyy-MM') AS mois, ROUND(SUM(quantite * prix), 2) AS ca
FROM ventes_raw
WHERE quantite > 0
GROUP BY pays, date_format(date_vente, 'yyyy-MM')
ORDER BY mois, ca DESC;
