with stg__salesorderheadersalesreason as (
    select *
    from {{ ref('stg__salesorderheadersalesreason') }}
),

stg__salesreason as (
    select *
    from {{ ref('stg__salesreason') }}
),

joined_data as (
    select *
    from stg__salesorderheadersalesreason
    left join stg__salesreason
        on stg__salesorderheadersalesreason.fk_salesreason = stg__salesreason.pk_salesreason
),

transformed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['fk_salesorder', 'fk_salesreason']
        ) }} as sk_salesreason,
        fk_salesorder,
        salesreason_name, 
        reasontype
    from joined_data
)

select *
from transformed