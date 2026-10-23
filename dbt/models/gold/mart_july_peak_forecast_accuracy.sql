{{ config(materialized='table', schema='gold', pre_hook="SET TIME ZONE 'UTC'") }}

with actuals as (
    select
        Zone as source_zone,
        cast(Year as int) as target_year,
        cast(`Load MW` as double) as actual_peak_mw
    from {{ source('silver', 'pjm_forecast_accuracy') }}
    where Metric = 'Historical July Peak'
),

forecasts as (
    select
        Zone as source_zone,
        cast(Year as int) as target_year,
        cast(substring(Metric, 1, 4) as int) as forecast_vintage_year,
        cast(`Load MW` as double) as forecast_peak_mw
    from {{ source('silver', 'pjm_forecast_accuracy') }}
    where Metric like '____ Load Forecast July Peak'
)

select
    forecast.source_zone,
    forecast.target_year,
    forecast.forecast_vintage_year,
    forecast.target_year - forecast.forecast_vintage_year as horizon_years,
    forecast.forecast_peak_mw,
    actual.actual_peak_mw,
    forecast.forecast_peak_mw - actual.actual_peak_mw as error_mw,
    100.0 * (forecast.forecast_peak_mw - actual.actual_peak_mw)
        / nullif(actual.actual_peak_mw, 0) as error_pct,
    100.0 * abs(forecast.forecast_peak_mw - actual.actual_peak_mw)
        / nullif(actual.actual_peak_mw, 0) as absolute_error_pct
from forecasts as forecast
left join actuals as actual
    on forecast.source_zone = actual.source_zone
    and forecast.target_year = actual.target_year
