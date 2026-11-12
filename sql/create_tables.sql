-- Apache Iceberg - E-commerce Lakehouse
-- Création du namespace et des tables Bronze, Silver et Gold.
-- À exécuter après configuration du catalogue Iceberg "local".

CREATE NAMESPACE IF NOT EXISTS local;

CREATE TABLE IF NOT EXISTS local.online_retail_bronze (
    Invoice STRING,
    StockCode STRING,
    Description STRING,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    Price DOUBLE,
    `Customer ID` DOUBLE,
    Country STRING,
    ingestion_timestamp TIMESTAMP,
    source_file STRING
)
USING iceberg;

-- La table Silver est créée par le job PySpark 03_bronze_to_silver.py.
-- Les tables Gold sont créées par le job PySpark 04_silver_to_gold.py.
-- Leur schéma doit rester cohérent avec les scripts d'implémentation.