-- dwh_sql_analytics-views.sql

-- Create Analysis Views for Witside_Production_Floor_DWH Tables
--We name Views with names startting with View_ for better tracking

--We use CREATE OR ALTER to allow changes if our business questions are modified.
--Initiate DB use
USE Witside_Production_Floor_DWH;
GO

-- View 1: View_Process_Cycle_Pairs
-- Purpose: Pairs every START (status_id=1) event with its subsequent STOP (status_id=3) event.
-----------------------------------------------------------------------

CREATE OR ALTER VIEW View_Process_Cycle_Pairs AS
SELECT
    FPE.production_line_id,
    FPE.event_time AS start_timestamp,
    -- Use LEAD() to fetch the timestamp of the *next* event
    LEAD(FPE.event_time, 1, NULL) OVER (
        PARTITION BY FPE.production_line_id
        ORDER BY FPE.event_time
    ) AS next_event_timestamp,
    -- Fetch the status_id of the next event
    LEAD(FPE.status_id, 1, NULL) OVER (
        PARTITION BY FPE.production_line_id
        ORDER BY FPE.event_time
    ) AS next_event_status_id
FROM
    Fact_ProcessEvents FPE
WHERE
    FPE.status_id IN (1, 3); -- Only consider START (1) and STOP (3) events
GO


-- View 2: View_Line_Process_Durations
-- Purpose: Calculates the duration of each completed START-STOP cycle.
-----------------------------------------------------------------------

CREATE OR ALTER VIEW View_Line_Process_Durations AS
SELECT
    VPP.production_line_id,
    VPP.start_timestamp,
    VPP.next_event_timestamp AS stop_timestamp,
    -- Calculate duration in seconds
    DATEDIFF(second, VPP.start_timestamp, VPP.next_event_timestamp) AS duration_seconds
FROM
    View_Process_Cycle_Pairs VPP
WHERE
    -- A cycle is complete if the current event is START (implied by the source query) 
    -- and the next event is STOP (status_id = 3).
    VPP.next_event_status_id = 3; 
GO


-- View 3: View_Line_Event_Sequences
-- Purpose: Pairs every event with the next event, including status names and gap time.
-----------------------------------------------------------------------

CREATE OR ALTER VIEW View_Line_Event_Sequences AS
SELECT
    FPE1.production_line_id,
    FPE1.event_time AS event_start_time,
    DS1.status_name AS event_start_status,
    
    -- Next Event details
    LEAD(FPE1.event_time, 1, NULL) OVER (
        PARTITION BY FPE1.production_line_id
        ORDER BY FPE1.event_time
    ) AS event_next_time,
    LEAD(FPE1.status_id, 1, NULL) OVER (
        PARTITION BY FPE1.production_line_id
        ORDER BY FPE1.event_time
    ) AS event_next_status_id,
    
    -- Calculate gap duration in seconds
    DATEDIFF(second, FPE1.event_time, 
        LEAD(FPE1.event_time, 1, NULL) OVER (
            PARTITION BY FPE1.production_line_id
            ORDER BY FPE1.event_time
        )
    ) AS gap_seconds
FROM
    Fact_ProcessEvents FPE1
JOIN
    Dim_Status DS1 ON FPE1.status_id = DS1.status_id
GO


-- View 4: View_Total_Uptime_Downtime
-- Purpose: Summarizes total uptime and downtime for each production line.
-----------------------------------------------------------------------

CREATE OR ALTER VIEW View_Total_Uptime_Downtime AS
WITH LineSequences AS (
    SELECT 
        production_line_id,
        event_start_status,
        gap_seconds
    FROM 
        View_Line_Event_Sequences
)
SELECT 
    production_line_id,
    -- Uptime (Operational Time): Include time following 'START' (1) and 'ON' (2) events.
    SUM(CASE 
        WHEN event_start_status IN ('START', 'ON') THEN gap_seconds 
        ELSE 0 
    END) AS total_uptime_seconds,
    
    -- Downtime is the duration when the line status was 'STOP' (3) until the next event
    SUM(CASE 
        WHEN event_start_status = 'STOP' THEN gap_seconds 
        ELSE 0 
    END) AS floor_downtime_seconds -- THIS MUST MATCH THE COLUMN NAME IN dwh_analytics.py
FROM 
    LineSequences
GROUP BY
    production_line_id;
GO