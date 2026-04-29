
--# SQL Server connection
-- 1. Create login at the SQL Server level
USE master;
GO

CREATE LOGIN powerbi_reader
WITH PASSWORD = 'Use_A_Strong_Password_Here!';
GO    

-- 2. Create user inside the reporting database
USE YourDatabaseName;
GO

CREATE USER powerbi_reader
FOR LOGIN powerbi_reader;
GO

-- 3. Grant read-only access
ALTER ROLE db_datareader ADD MEMBER powerbi_reader;
GO


--# in Prod
USE YourDatabaseName;
GO

-- Create a custom role
CREATE ROLE powerbi_report_reader;
GO

-- Grant read access only to reporting schema
GRANT SELECT ON SCHEMA::dbo TO powerbi_report_reader;
GO

-- Add user to custom role
ALTER ROLE powerbi_report_reader ADD MEMBER powerbi_reader;
GO

GRANT SELECT ON SCHEMA::reporting TO powerbi_report_reader;