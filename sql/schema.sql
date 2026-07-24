-- sql/schema.sql
-- Run against the target SQL Server instance (sqlcmd -S <host> -i schema.sql)

IF DB_ID('IoTSensors') IS NULL
BEGIN
    CREATE DATABASE IoTSensors;
END
GO

USE IoTSensors;
GO

IF OBJECT_ID('dbo.SensorReadings', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SensorReadings (
        Id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        DeviceId        VARCHAR(32)   NOT NULL,
        SensorType      VARCHAR(16)   NOT NULL,
        Value           FLOAT         NOT NULL,
        ReadingTimeUtc  DATETIME2(3)  NOT NULL,
        SequenceNo      INT           NOT NULL,
        IngestedAtUtc   DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

-- Covering, filtered indexes tuned for the dashboard's hot-path queries
-- (latest N readings per device, and windowed aggregates per sensor type).
-- At ~50k rows/day these keep query plans as index seeks instead of scans,
-- which is what keeps dashboard queries sub-second as data accumulates.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SensorReadings_Device_Time'
               AND object_id = OBJECT_ID('dbo.SensorReadings'))
    CREATE NONCLUSTERED INDEX IX_SensorReadings_Device_Time
        ON dbo.SensorReadings (DeviceId, ReadingTimeUtc DESC)
        INCLUDE (SensorType, Value);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SensorReadings_Type_Time'
               AND object_id = OBJECT_ID('dbo.SensorReadings'))
    CREATE NONCLUSTERED INDEX IX_SensorReadings_Type_Time
        ON dbo.SensorReadings (SensorType, ReadingTimeUtc DESC)
        INCLUDE (DeviceId, Value);
GO

-- Rolling aggregate table, refreshed by a scheduled job (see scripts/rollup.sql
-- pattern below) so summary dashboard widgets never scan raw rows at all.
IF OBJECT_ID('dbo.SensorReadingsHourly', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SensorReadingsHourly (
        DeviceId    VARCHAR(32)  NOT NULL,
        SensorType  VARCHAR(16)  NOT NULL,
        HourUtc     DATETIME2(0) NOT NULL,
        AvgValue    FLOAT        NOT NULL,
        MinValue    FLOAT        NOT NULL,
        MaxValue    FLOAT        NOT NULL,
        ReadingCount INT         NOT NULL,
        PRIMARY KEY (DeviceId, SensorType, HourUtc)
    );
END
GO

-- Example rollup statement, run every hour via a SQL Agent job / k8s CronJob.
-- INSERT INTO dbo.SensorReadingsHourly ...
--     SELECT DeviceId, SensorType, DATEADD(HOUR, DATEDIFF(HOUR, 0, ReadingTimeUtc), 0),
--            AVG(Value), MIN(Value), MAX(Value), COUNT(*)
--     FROM dbo.SensorReadings
--     WHERE ReadingTimeUtc >= DATEADD(HOUR, -1, SYSUTCDATETIME())
--     GROUP BY DeviceId, SensorType, DATEADD(HOUR, DATEDIFF(HOUR, 0, ReadingTimeUtc), 0);
