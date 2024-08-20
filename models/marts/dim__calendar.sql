{{ config(
    materialized='table'
    , partition_by={
        'field': 'metric_date'
        , 'data_type': 'date'
    }
    , cluster_by=['metric_date', 'week', 'month', 'year']
    , meta = {
        'area': 'marketing'
        , 'system': 'dbt_utils'
        , 'table': 'dim_calendar'
        , 'category': 'dimension'
    }
) }}

with date_series as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2015-12-31' as date)"
    )}}
)

, date_columns as (
    select distinct
        cast(date_day as date) as metric_date
        , extract(day from date_day) as dayofmonth
        , extract(month from date_day) as month
        , extract(year from date_day) as year
        , extract(quarter from date_day) as quarter
        , extract(dayofyear from date_day) as dayofyear
        , extract(week from date_day) as isoweek
        , case
            when extract(day from date_day) != 1 and extract(dayofweek from date_day) = 1
                then extract(week from date_day)-1
            else extract(week from date_day)
        end as week
        , case
            when extract(dayofweek from date_day) = 1 then 'Domingo'
            when extract(dayofweek from date_day) = 2 then 'Segunda'
            when extract(dayofweek from date_day) = 3 then 'Terça'
            when extract(dayofweek from date_day) = 4 then 'Quarta'
            when extract(dayofweek from date_day) = 5 then 'Quinta'
            when extract(dayofweek from date_day) = 6 then 'Sexta'
            when extract(dayofweek from date_day) = 7 then 'Sábado'
        end as dayofweek
        , case
            when extract(month from date_day) = 1 then 'Jan'
            when extract(month from date_day) = 2 then 'Fev'
            when extract(month from date_day) = 3 then 'Mar'
            when extract(month from date_day) = 4 then 'Abr'
            when extract(month from date_day) = 5 then 'Mai'
            when extract(month from date_day) = 6 then 'Jun'
            when extract(month from date_day) = 7 then 'Jul'
            when extract(month from date_day) = 8 then 'Ago'
            when extract(month from date_day) = 9 then 'Set'
            when extract(month from date_day) = 10 then 'Out'
            when extract(month from date_day) = 11 then 'Nov'
            when extract(month from date_day) = 12 then 'Dez'
        end as fullmonth
    from date_series
)

, transformed as (
    select
        *
        , concat('W', cast(week as string), '-', cast(year as string)) as week_year
    from date_columns
)

select *
from transformed