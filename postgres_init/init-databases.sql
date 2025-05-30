-- Create databases if they don't exist
SELECT 'CREATE DATABASE rtv' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'rtv');
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow');
