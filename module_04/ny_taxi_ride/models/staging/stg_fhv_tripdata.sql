{{
  config(materialized='view')
}}

with tripdata as
(
  select *
  from {{ source('staging','fhv_tripdata') }}
  -- where EXTRACT(YEAR FROM pickup_datetime) = 2019
)
select
  -- identifiers
  {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
  dispatching_base_num,
  {{ dbt.safe_cast('"PUlocationID"', api.Column.translate_type("integer")) }} as pickup_locationid,
  {{ dbt.safe_cast('"DOlocationID"', api.Column.translate_type("integer")) }} as dropoff_locationid,

  -- timestamps
  cast(pickup_datetime as timestamp) as pickup_datetime,
  cast("dropOff_datetime" as timestamp) as dropoff_datetime,
  'Affiliated_base_number' as affiliated_base_number,
  'SR_Flag' as sr_flag
from tripdata
