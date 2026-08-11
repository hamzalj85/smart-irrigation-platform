-- Table de faits : une ligne par appareil et par heure.
-- C'est la granularite que consomment Grafana et le modele ML.

with readings as (

    select * from {{ ref('stg_telemetry') }}

)

select
    device_id,
    site_id,
    date_trunc('hour', measured_at)                as hour_start,
    cast(date_trunc('hour', measured_at) as date)  as measured_date,

    count(*)                                       as reading_count,
    round(avg(soil_moisture_pct), 2)               as avg_soil_moisture_pct,
    round(min(soil_moisture_pct), 2)               as min_soil_moisture_pct,
    round(max(soil_moisture_pct), 2)               as max_soil_moisture_pct,
    round(max(soil_moisture_pct) - min(soil_moisture_pct), 2) as soil_moisture_range_pct,

    round(avg(air_temp_c), 2)                      as avg_air_temp_c,
    round(avg(air_humidity_pct), 2)                as avg_air_humidity_pct,
    round(min(battery_v), 3)                       as min_battery_v

from readings
group by 1, 2, 3, 4