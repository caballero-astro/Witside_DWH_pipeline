USE Witside_Production_Floor_DWH;
GO

SELECT * FROM Dim_Status;
GO

SELECT * FROM Dim_ProductionLine;
GO

SELECT * FROM Fact_ProcessEvents;
GO

SELECT * FROM Staging_Reject_Events;
GO

--TRUNCATE TABLE Fact_ProcessEvents;
--GO

--TRUNCATE TABLE Staging_Reject_Events;
--GO

--DROP TABLE Fact_ProcessEvents
--GO
