-- SQL Homework

-- Question 3. Count records 

SELECT COUNT(*) AS trip_count
FROM "hw_green_tripdata_2019-09"
WHERE 
    DATE(lpep_pickup_datetime) = '2019-09-18'
    AND DATE(lpep_dropoff_datetime) = '2019-09-18';
	
-- Question 4. Largest trip for each day
SELECT
    DATE(lpep_pickup_datetime) AS pick_up_day,
    MAX(trip_distance) AS max_trip_distance
FROM
    "hw_green_tripdata_2019-09"
GROUP BY
    pick_up_day
ORDER BY max_trip_distance DESC;

-- Question 5. Three biggest pick up Boroughs
SELECT 
    lp."Borough" AS area,
    SUM(total_amount) as ta
FROM 
    "hw_green_tripdata_2019-09" AS trips
JOIN 
    lookups AS lp ON trips."PULocationID" = lp."LocationID"
WHERE 
    trips.passenger_count IS NOT NULL 
    AND DATE(trips.lpep_pickup_datetime) = '2019-09-18'
GROUP BY 
    area
HAVING 
    SUM(total_amount) >= 50000
ORDER BY 
    ta DESC;

-- Question 6. Largest tips
 SELECT 
    lp_pickup."Zone" AS pickup_zone,
    lp_dropoff."Zone" AS dropoff_zone,
    trips.tip_amount AS tips,
    lp_dropoff.service_zone,
    trips."DOLocationID"
FROM 
    "hw_green_tripdata_2019-09" AS trips
JOIN 
    lookups AS lp_pickup ON trips."PULocationID" = lp_pickup."LocationID"
JOIN
    lookups AS lp_dropoff ON trips."DOLocationID" = lp_dropoff."LocationID"
WHERE 
    lp_pickup."Zone" = 'Astoria'
ORDER BY 
    tips DESC;
