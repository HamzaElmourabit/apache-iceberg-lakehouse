-- Snapshot actuel
SELECT *
FROM local.iceberg_operations_demo;

-- Historique des snapshots
SELECT *
FROM local.iceberg_operations_demo.snapshots;

-- Time Travel avec un snapshot
SELECT *
FROM local.iceberg_operations_demo
VERSION AS OF <snapshot_id>;