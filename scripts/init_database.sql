/*
=============================================================
Create Database and Schemas (MySQL Version)
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking 
    if it already exists. If the database exists, it is dropped and recreated. 
    
    NOTE:
    MySQL treats a SCHEMA as a DATABASE. Unlike SQL Server, MySQL does not support 
    multiple schemas inside a single database. Therefore, creating schemas such as 
    'bronze', 'silver', and 'gold' will create *separate databases.*
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in that database will be permanently deleted.
    Proceed with caution and ensure you have proper backups before running this script.
*/

-- ------------------------------------------------------------
-- Drop and recreate the 'DataWarehouse' database
-- ------------------------------------------------------------

DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;

-- Switch to the new database
USE DataWarehouse;

-- ------------------------------------------------------------
-- Create Schemas (MySQL creates separate databases)
-- ------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

/*
=============================================================
End of Script
=============================================================
*/
