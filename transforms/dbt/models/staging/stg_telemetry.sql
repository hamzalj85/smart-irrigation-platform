-- Staging layer: renaming and typing, no business logic.
-- Materialised as a view: always reflects the latest Parquet written by Spark.

with source as (

    select * from {{ source('lake', 'telemetry') }}

)

select
    device_id,
    site_id,
    cast(ts as timestamp)           as measured_at,
    cast(date as date)              as measured_date,
    soil_moisture_pct,
    soil_raw,
    air_temp_c,
    air_humidity_pct,
    battery_v,
    fw_version
from source