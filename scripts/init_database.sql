-- =============================================================
-- Create Database and Schemas in PostgreSQL
-- =============================================================
-- WARNING:
-- This script will drop and recreate the 'datawarehouse' database.
-- All data in the database will be permanently deleted.

-- Drop the database if it exists
DROP DATABASE IF EXISTS datawarehouse;

-- Create the new database
CREATE DATABASE datawarehouse;

-- ==============================
-- IMPORTANT MANUAL STEP:
-- After creating the database, you must CONNECT to 'datawarehouse'
-- in your client tool (pgAdmin: select database > Query Tool, psql: \c datawarehouse).
-- All following commands must be run inside the 'datawarehouse' database!
-- ==============================

-- Create schemas (once connected to datawarehouse)
DROP SCHEMA IF EXISTS bronze CASCADE;
CREATE SCHEMA bronze;

DROP SCHEMA IF EXISTS silver CASCADE;
CREATE SCHEMA silver;

DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;
