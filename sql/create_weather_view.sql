-- 5CLOU - Paris Weather Analytics
-- Azure Synapse Analytics / Serverless SQL
-- Reproduction de la vue dbo.vw_weather_daily (source : definition exportee)
-- ATTENTION : la vue existe deja dans le workspace actuel.
-- Ne pas executer ce script sur la base existante sans preparation.

USE WeatherDB;
GO

CREATE VIEW dbo.vw_weather_daily
AS
SELECT
    date,
    avg_temperature_c,
    min_temperature_c,
    max_temperature_c,
    avg_humidity_pct,
    avg_wind_speed_kmh,
    max_wind_speed_kmh,
    measurement_count
FROM OPENROWSET(
    BULK 'https://st5clouweather.dfs.core.windows.net/synapse/gold/weather_daily/',
    FORMAT = 'DELTA'
) AS weather;
