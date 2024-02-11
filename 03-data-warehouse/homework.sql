DECLARE GS_BUCKET_PATH STRING DEFAULT @GS_BUCKET_PATH; -- env var set on local

-- Create an external table refer to gcs
CREATE OR REPLACE EXTERNAL TABLE `etlwork.taxi_rides.green_taxi`
OPTIONS (
  FORMAT='PARQUET',
  uris=[GS_BUCKET_PATH]
);

-- Create an internal table in BigQuery using the external table
CREATE OR REPLACE TABLE `etlwork.taxi_rides.green_taxi_non_partitioned`
AS
SELECT *
FROM `etlwork.taxi_rides.green_taxi`;


SELECT *  FROM `etlwork.taxi_rides.green_taxi_non_partitioned`;


-- Query for the external table
SELECT COUNT(DISTINCT PULocationID) AS distinct_puid_count_ex
FROM `etlwork.taxi_rides.green_taxi`;

-- Query for the internal table
SELECT COUNT(DISTINCT PULocationID) AS distinct_puid_count_in
FROM `etlwork.taxi_rides.green_taxi_non_partitioned`;


SELECT COUNT(*) AS fare_count
FROM `etlwork.taxi_rides.green_taxi_non_partitioned`
WHERE fare_amount = 0;


-- Create the partitioned and clustered table
CREATE OR REPLACE TABLE `etlwork.taxi_rides.green_taxi_partitioned`
PARTITION BY DATE(pickup_date)
CLUSTER BY PUlocationID
AS
SELECT
  *,
  TIMESTAMP_SECONDS(CAST(lpep_pickup_datetime/1000000000 AS INT64)) AS pickup_date
FROM
  `etlwork.taxi_rides.green_taxi`;


-- Query for the non-partitioned table
SELECT DISTINCT PULocationID
FROM `etlwork.taxi_rides.green_taxi_non_partitioned`
WHERE DATE(TIMESTAMP_SECONDS(CAST(lpep_pickup_datetime/1000000000 AS INT64))) BETWEEN '2022-06-01' AND '2022-06-30';

-- Query for the partitioned table
SELECT DISTINCT PULocationID
FROM `etlwork.taxi_rides.green_taxi_partitioned`
WHERE DATE(pickup_date) BETWEEN '2022-06-01' AND '2022-06-30';
