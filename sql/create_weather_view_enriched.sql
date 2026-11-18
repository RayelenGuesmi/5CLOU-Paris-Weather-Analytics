-- 5CLOU - Paris Weather Analytics
-- Azure Synapse Analytics / Serverless SQL
-- Vue exposant le GOLD enrichi (historique Open-Meteo Archive + collecte ADF),
-- interrogée en DirectQuery par le rapport Power BI.
-- Définition en vigueur au 18/11/2026 (récupérée via sys.sql_modules).

USE WeatherDB;
GO

CREATE OR ALTER VIEW dbo.vw_weather_daily_enriched
AS
SELECT
    CAST([date] AS date) AS [date],
    CONVERT(char(7), CAST([date] AS date), 126) AS AnneeMois,
    avg_temperature_c,
    min_temperature_c,
    max_temperature_c,
    avg_humidity_pct,
    avg_wind_speed_kmh,
    max_wind_speed_kmh,
    CAST(measurement_count AS bigint) AS measurement_count,
    data_source
FROM OPENROWSET(
    BULK 'https://st5clouweather.dfs.core.windows.net/synapse/gold/weather_daily_enriched/',
    FORMAT = 'DELTA'
) AS weather;
