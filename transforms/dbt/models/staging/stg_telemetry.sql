-- Couche de staging : renommage, typage, aucune logique metier.
-- Materialise en vue : reflete toujours les derniers Parquet ecrits par Spark.

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