/*
==============================
Creating Database and Schemas
==============================

Script Purpose:
	Firstly, if Database named 'DataWarehouse' exist, script will drop and recreate it.
	Then divided it into three schemas named 'bronze', 'silver' and 'gold'.

WARNING!
	If 'DataWarehouse' exist, this script will drop it!
*/

USE master;
GO


-- Drop and Recreate DB: 'DataWarehouse'
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
USE DataWarehouse;
GO

-- Create Schemas: 'bronze', 'silver' and 'gold' 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
