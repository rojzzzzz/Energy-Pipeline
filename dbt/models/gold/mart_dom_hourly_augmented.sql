{{ config(materialized='table', schema='gold', pre_hook="SET TIME ZONE 'UTC'") }}

with demand as (
    select
        cast(period as timestamp_ntz) - interval 1 hour as interval_start_utc,
        cast(value as double) as demand_mwh
    from {{ source('silver', 'hourly_pjm_dom_demand') }}
    where parent = 'PJM' and subba = 'DOM'
),

bounds as (
    select
        min(interval_start_utc) as first_hour,
        max(interval_start_utc) as last_hour
    from demand
),

hours as (
    select explode(sequence(first_hour, last_hour, interval 1 hour)) as interval_start_utc
    from bounds
),

local_hours as (
    select
        interval_start_utc,
        convert_timezone('UTC', 'America/New_York', interval_start_utc) as local_timestamp
    from hours
),

weather as (
    select
        cast(date_trunc('hour', DATE) as timestamp_ntz) as interval_start_utc,
        avg(temperature) as temperature_c,
        avg(dew_point_temperature) as dew_point_c,
        avg(relative_humidity) as relative_humidity_pct,
        avg(wind_speed) as wind_speed_mps,
        count(*) as weather_observation_count,
        count(temperature) as temperature_observation_count
    from {{ source('silver', 'noaa_GHCNh') }}
    where STATION = 'USW00093738'
    group by date_trunc('hour', DATE)
),

prices as (
    select
        cast(date_trunc('month', period) as date) as price_month,
        cast(price as double) as price_cents_per_kwh
    from {{ source('silver', 'monthly_va_residential_price') }}
    where stateid = 'VA' and sectorid = 'RES'
)

select
    'PJM_DOM' as area_key,
    hours.interval_start_utc,
    hours.interval_start_utc + interval 1 hour as interval_end_utc,
    cast(hours.local_timestamp as date) as local_date,
    hour(hours.local_timestamp) as local_hour,
    timestampdiff(MINUTE, hours.interval_start_utc, hours.local_timestamp) as utc_offset_minutes,
    dayofweek(hours.local_timestamp) in (1, 7) as is_weekend,
    demand.demand_mwh,
    demand.demand_mwh / 1.0 as average_load_mw,
    'USW00093738' as station_key,
    weather.temperature_c,
    weather.dew_point_c,
    weather.relative_humidity_pct,
    weather.wind_speed_mps,
    coalesce(weather.weather_observation_count, 0) as weather_observation_count,
    coalesce(weather.temperature_observation_count, 0) as temperature_observation_count,
    prices.price_month,
    prices.price_cents_per_kwh as va_residential_price_cents_per_kwh,
    demand.demand_mwh is null as demand_missing,
    weather.interval_start_utc is null as weather_missing,
    weather.temperature_c is null as temperature_missing,
    prices.price_cents_per_kwh is null as price_missing
from local_hours as hours
left join demand
    on hours.interval_start_utc = demand.interval_start_utc
left join weather
    on hours.interval_start_utc = weather.interval_start_utc
left join prices
    on cast(date_trunc('month', hours.local_timestamp) as date) = prices.price_month
