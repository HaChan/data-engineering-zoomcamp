-- Question 1 find the pair of taxi zones with the highest average trip time
CREATE MATERIALIZED VIEW highest_avg_trip_time_between_zones AS
  with trip_between_zones as (
    select
      pickup_zone.zone as pu_zone,
      dropoff_zone.zone as do_zone,
      avg(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time,
      max(tpep_dropoff_datetime - tpep_pickup_datetime) as max_trip_time,
      min(tpep_dropoff_datetime - tpep_pickup_datetime) as min_trip_time
    from
      trip_data
    join taxi_zone pickup_zone
      on trip_data.pulocationid = pickup_zone.location_id
    join taxi_zone dropoff_zone
      on trip_data.dolocationid = dropoff_zone.location_id
    group by
      pickup_zone.zone, dropoff_zone.zone
  ), highest_avg_trip_time as (
    select max(avg_trip_time) max_trip_time
    from
      trip_between_zones
  )
  select tbz.*
  from
    trip_between_zones tbz,
    highest_avg_trip_time hat
  where
    tbz.avg_trip_time = hat.max_trip_time;

-- Question 2 find number of trip for the pair of taxi zones with the highest average trip time
CREATE MATERIALIZED VIEW count_highest_avg_trip_time AS
  with trip_between_zones as (
    select
      pickup_zone.zone as pu_zone,
      dropoff_zone.zone as do_zone,
      avg(tpep_dropoff_datetime - tpep_pickup_datetime) as avg_trip_time,
      max(tpep_dropoff_datetime - tpep_pickup_datetime) as max_trip_time,
      min(tpep_dropoff_datetime - tpep_pickup_datetime) as min_trip_time
    from
      trip_data
    join taxi_zone pickup_zone
      on trip_data.pulocationid = pickup_zone.location_id
    join taxi_zone dropoff_zone
      on trip_data.dolocationid = dropoff_zone.location_id
    group by
      pickup_zone.zone, dropoff_zone.zone
  ), highest_avg_trip_time as (
    select max(avg_trip_time) max_trip_time
    from
      trip_between_zones
  )
  select count(*)
  from
    trip_between_zones tbz,
    highest_avg_trip_time hat
  where
    tbz.avg_trip_time = hat.max_trip_time;

-- Question 3 top 3 busiest zones in terms of number of pickups
CREATE MATERIALIZED VIEW busiest_zones AS
  select tz.zone, count(*) as pickup_count
  from trip_data t
  join taxi_zone tz
    on t.pulocationid = tz.location_id
  where t.tpep_pickup_datetime BETWEEN
    (select max(tpep_pickup_datetime - INTERVAL '17 hours') as latest_time from trip_data)
    AND
    (select max(tpep_pickup_datetime) as latest_time from trip_data)
  group by tz.zone
  order by pickup_count desc
  limit 3;

