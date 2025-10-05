/*
=============================================================
Create Databases for Medallion Architecture (MySQL version)
=============================================================
Script Purpose:
    This script creates three separate databases — 'bronze', 'silver', and 'gold' — 
    to represent the medallion stages. This mirrors the schemas used in SQL Server.
    
WARNING:
    Running this script will drop and recreate these databases if they already exist. 
    All data will be permanently deleted. Proceed with caution and ensure you have backups.
*/

-- Drop and recreate each medallion stage database
DROP DATABASE IF EXISTS bronze;
CREATE DATABASE bronze;

DROP DATABASE IF EXISTS silver;
CREATE DATABASE silver;

DROP DATABASE IF EXISTS gold;
CREATE DATABASE gold;
