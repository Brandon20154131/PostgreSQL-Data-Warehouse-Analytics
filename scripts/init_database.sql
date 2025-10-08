/*
=============================================================
Create Database and Schemas for Medallion Architecture 
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
    
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Connect to the default database (postgres) before running this part
-- Drop the database if it exists
DROP DATABASE IF EXISTS "DataWarehouse";

-- Create the new database
CREATE DATABASE "DataWarehouse";

-- Now connect to the new "DataWarehouse" database manually in pgAdmin before running the next section

-- Create each medallion stage database
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
