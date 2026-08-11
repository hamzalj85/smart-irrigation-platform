-- Dimension : une ligne par appareil, son etat et son historique.
-- Alimente la supervision de parc et les jointures analytiques.

with readings as (

    select * from {{ ref('stg_telemetry') }}

)

select
    device_id,
    max(site_id)                       as site_id,
    max(fw_version)                    as fw_version,

    min(measured_at)                   as first_seen_at,
    max(measured_at)                   as last_seen_at,
    date_diff('minute', max(measured_at), current_timestamp) as minutes_since_last_reading,

    count(*)                           as total_readings,
    count(distinct measured_date)      as active_days,

    round(min(battery_v), 3)           as min_battery_v,
    round(avg(soil_moisture_pct), 2)   as avg_soil_moisture_pct,
    round(min(soil_moisture_pct), 2)   as min_soil_moisture_pct,
    round(max(soil_moisture_pct), 2)   as max_soil_moisture_pct

from readings
group by 1