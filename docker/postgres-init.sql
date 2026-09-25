-- Runs once when the Postgres volume is first created. The default database
-- (POSTGRES_DB=airflow) holds Airflow metadata; analytics data lives in its
-- own database so the two can be backed up / scaled independently.
CREATE DATABASE github_analytics;
