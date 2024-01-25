-- How many taxi trips were totally made on September 18th 2019?
SELECT COUNT(*)
FROM green_taxi_data
WHERE DATE(lpep_pickup_datetime) = '2019-09-18';


-- Which was the pick up day with the largest trip distance

select pickup_date, total_distance
from (
  select DATE(lpep_pickup_datetime) as pickup_date,
   sum(trip_distance) total_distance
  from green_taxi_data
  group by 1
) pickup order by total_distance desc limit 1

-- Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

select borough, b_total
from (
  select sum(g.total_amount) b_total, z."Borough" borough
  from green_taxi_data g
  join taxi_zone_lookup z on g."PULocationID" = z."LocationID"
  where z."Zone" is not null and Date(g.lpep_pickup_datetime) = '2019-09-18'
  group by z."Borough"
) b
order by b_total desc
limit 5

-- For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip

select b_tip, z."Zone"
from (
  select max(g.tip_amount) b_tip, g."DOLocationID"
  from green_taxi_data g
  join taxi_zone_lookup z on g."PULocationID" = z."LocationID"
  where z."Zone" = 'Astoria' and TO_CHAR(g.lpep_pickup_datetime, 'YYYY-MM') = '2019-09'
  group by g."DOLocationID"
 ) tip
join taxi_zone_lookup z on tip."DOLocationID" = z."LocationID"
order by b_tip desc
limit 5
