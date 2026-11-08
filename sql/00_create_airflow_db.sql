-- Airflow keeps its own metadata (DAG runs, task states) in a separate database
-- on the same Postgres server, so it never mixes with the mobility data.
CREATE DATABASE airflow;
