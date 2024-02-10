-- Q1
SELECT count(*) FROM `de-course-412219.green_taxi_trip.green_taxi`;
-- Q2
SELECT count(*) FROM (SELECT PULocationID FROM `de-course-412219.green_taxi_trip.green_taxi` group by PULocationID) green_taxi_pu;
-- Q3
SELECT count(*) FROM `de-course-412219.green_taxi_trip.green_taxi` where fare_amount = 0;
-- Q4
CREATE OR REPLACE TABLE de-course-412219.green_taxi_trip.green_tripdata_partitoned_clustered
PARTITION BY
  DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID  AS
SELECT * FROM de-course-412219.green_taxi_trip.green_taxi;
-- Q5
FROM `de-course-412219.green_taxi_trip.green_taxi`
where DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30'
group by PULocationID) green_taxi_pu;

SELECT count(*) FROM
(SELECT PULocationID
FROM `de-course-412219.green_taxi_trip.green_tripdata_partitoned_clustered`
where DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' and '2022-06-30'
group by PULocationID) green_taxi_pu;
