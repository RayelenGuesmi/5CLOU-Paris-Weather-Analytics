-- 5CLOU - Paris Weather Analytics
-- Azure Synapse Analytics / Serverless SQL
-- Vue exposant le GOLD enrichi (historique Open-Meteo Archive + collecte ADF)
-- Définition récupérée via sys.sql_modules le 18/11/2026.
-- ATTENTION : la vue existe déjà dans le workspace actuel.
-- Ne pas exécuter ce script sur la base existante sans préparation.

USE WeatherDB;
GO

CREATE VIEW dbo.vw_weather_daily_enriched
AS
SELECT
    CAST([date] AS date) AS [date],
    avg_temperature_c,
    min_temperature_c,
    max_temperature_c,
    avg_humidity_pct,
    avg_wind_speed_kmh,
    max_wind_speed_kmh,
    CAST(measurement_count AS bigint) AS measurement_count
FROM OPENROWSET(
    BULK 'https://st5clouweather.dfs.core.windows.net/synapse/gold/weather_daily_enriched/',
    FORMAT = 'DELTA'
) AS weather;
