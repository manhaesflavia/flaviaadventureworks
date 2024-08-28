with stg__salesorderheadersalesreason as (
    select *
    from {{ ref('stg__salesorderheadersalesreason') }}
)

, stg__salesreason as (
    select *
    from {{ ref('stg__salesreason') }}
)

, reason_aggregated as (
    select 
        stg__salesorderheadersalesreason.fk_salesreason
        , stg__salesorderheadersalesreason.fk_salesorder          
        , stg__salesreason.pk_salesreason       
        , stg__salesreason.salesreason_name
        , stg__salesreason.reasontype
    from stg__salesorderheadersalesreason
    left join stg__salesreason 
        on stg__salesorderheadersalesreason.fk_salesreason = stg__salesreason.pk_salesreason 
)

, transformed as (
    select
        fk_salesorder        
        , string_agg(salesreason_name, ', ') as reason_name_aggregated
    from reason_aggregated
    group by fk_salesorder
)

select *
from transformed