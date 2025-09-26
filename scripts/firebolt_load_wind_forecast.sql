-- SQL script to load staged GFS CSV from S3 into a Firebolt table
-- Edit the S3 URI and table names as needed before running in your Firebolt console.

-- Create final table (non-destructive: if you prefer replace, change to CREATE OR REPLACE TABLE)
CREATE TABLE IF NOT EXISTS wind_forecast (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  obs_date DATE,
  obs_hour INT,
  wind_u DOUBLE,
  wind_v DOUBLE,
  wind_speed DOUBLE,
  wind_heading_rad DOUBLE,
  location GEOGRAPHY
);

-- Create or replace an external table over the staged CSV in S3
-- Replace <S3_URI> with the actual S3 path to the CSV (e.g. s3://wind-data-sergio/gribfiles/staged_20250923.csv)
-- Replace <CSV_FILENAME> below with the staged CSV filename produced by the extractor (format: YYYY-MM-DD_HH.csv)
CREATE EXTERNAL TABLE "ext_gfs_points_staged" (
  "lat" DOUBLE PRECISION NULL,
  "lon" DOUBLE PRECISION NULL,
  "time" TEXT NULL,
  "u10" INTEGER NULL,
  "v10" INTEGER NULL,
  "obs_date" TEXT NULL,
  "obs_hour" INTEGER NULL,
  "forecast_hour" INTEGER NULL
) URL = 's3://wind-data-sergio/gribfiles/' CREDENTIALS = (
  AWS_ACCESS_KEY_ID = '***' AWS_SECRET_ACCESS_KEY = '***'
) PATTERN = '*.csv' TYPE = CSV SKIP_HEADER_ROWS = TRUE

-- Example LOCATION (edit as needed):
-- LOCATION = 's3://wind-data-sergio/gribfiles/2025-09-23_00.csv'
LOCATION = 's3://wind-data-sergio/gribfiles/<CSV_FILENAME>'
FILE_FORMAT = (type='csv' header=true);

-- Insert from external table into final table, computing speed and heading in SQL
INSERT INTO wind_forecast (obs_date, obs_hour, wind_u, wind_v, wind_speed, wind_heading_rad, location)
SELECT
  TRY_CAST(obs_date AS DATE) AS obs_date,
  obs_hour,
  wind_u,
  wind_v,
  sqrt((wind_u)*(wind_u) + (wind_v)*(wind_v)) AS wind_speed,
  ((atan2(-(wind_u), -(wind_v)) + 2 * 3.141592653589793) % (2 * 3.141592653589793)) AS wind_heading_rad,
  location_wkt
FROM ext_wind_staged;

-- Drop external table when done
DROP EXTERNAL TABLE IF EXISTS ext_wind_staged;
