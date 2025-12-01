-- dwh_sql_schema-tables.sql

-- Create Witside_Production_Floor_DWH Tables

--Initiate DB use
USE Witside_Production_Floor_DWH;
GO

----Clean up part--
-----------------------------------------------------------------------
-- 1. CLEANUP (Ensuring Idempotency for schema setup)
-- Drops all tables to ensure a clean slate when SETUP_SCHEMA=True
-----------------------------------------------------------------------

-- Drop Fact table first (due to FK dependencies on Dim tables)
IF OBJECT_ID('dbo.Fact_ProcessEvents', 'U') IS NOT NULL 
    DROP TABLE dbo.Fact_ProcessEvents; 
GO

-- Drop Dimension tables and Staging table
IF OBJECT_ID('dbo.Dim_ProductionLine', 'U') IS NOT NULL 
    DROP TABLE dbo.Dim_ProductionLine; 
GO

IF OBJECT_ID('dbo.Dim_Status', 'U') IS NOT NULL 
    DROP TABLE dbo.Dim_Status; 
GO

IF OBJECT_ID('dbo.Staging_Reject_Events', 'U') IS NOT NULL 
    DROP TABLE dbo.Staging_Reject_Events; 
GO

--Overview of initial input data:
-- 3-column table: 
-- Column1 = production_line_id (string of form: gr-np-55)
-- Column2 = status (string, values are ON, START, STOP)
-- Column3 = timestamp (TIMESTAMP, values of form: 2020-10-07T06:00:00)

-- Create DW Tables:
-- We create DW tables folowing STAR schema
-- We create 2 dimension tables and 1 fact table

--Dimenension Table 1: Dim_Status 
--Lookup table to transform status from strings (status_name) to integers (status_id)
CREATE TABLE Dim_Status(
status_id TINYINT PRIMARY KEY, 
status_name VARCHAR(6) NOT NULL UNIQUE
);
GO

-- Insert the fixed status mappings (DML for setup)
INSERT INTO Dim_Status (status_id, status_name) VALUES
(1, 'START'),
(2, 'ON'),
(3, 'STOP');
GO

--Dimenension Table 2: Dim_ProductionLine
--Table that lists all unique production lines encountered in the data
CREATE TABLE Dim_ProductionLine(
production_line_id VARCHAR(10) PRIMARY KEY
);
GO


--Fact table: Fact_ProcessEvents
--
CREATE TABLE Fact_ProcessEvents (
    --A simple, single-column and unique (appropriate constaint below) identifier for efficient joins and internal DWH operations
    event_key BIGINT IDENTITY(1,1) NOT NULL, --To be used as surrogate key.
    production_line_id VARCHAR(10) NOT NULL,
    status_id TINYINT NOT NULL,
    event_time DATETIME2(0) NOT NULL,
    --We use Composite Primary Key production_line_id+event_time
    --Will be used for easy "no duplicates" handling rule
    --Allows use of simple INSERT statements from Pandas and rely on the database rollback for integrity 
    --when loading data from produciton records (eg csv file).
    CONSTRAINT PK_Fact_ProcessEvents PRIMARY KEY CLUSTERED 
    (
        production_line_id ASC,
        event_time ASC 
    ),
    
    -- FOREIGN KEYS/Constraints
    
    -- Define a UNIQUE NONCLUSTERED index for the Surrogate Key (event_key) 
    -- to optimize performance and treat it as a unique identifier.
    CONSTRAINT UQ_Fact_ProcessEvents_SK UNIQUE NONCLUSTERED (event_key),
    
    FOREIGN KEY (production_line_id) REFERENCES Dim_ProductionLine(production_line_id),
    FOREIGN KEY (status_id) REFERENCES Dim_Status(status_id)
);
GO


-- 4. Staging Table: Staging_Reject_Events
-- This is create to quarantine rows that violate data quality rules
-- The main motivation is to catch cases where the natural order START -> ON -> STOP is not maintained
-- Accurate measurement of uptimes/downtimes require measuring time is complete cycles (From START to STOP)!

CREATE TABLE Staging_Reject_Events (
    reject_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    production_line_id VARCHAR(10),
    status_name VARCHAR(6),
    event_time DATETIME2(0),
    error_reason VARCHAR(255) NOT NULL,
    rejection_timestamp DATETIME2(0) DEFAULT GETDATE()
);
GO
