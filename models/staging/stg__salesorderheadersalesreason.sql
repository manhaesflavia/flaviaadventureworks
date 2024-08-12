with source as (
    select
        salesorderid as fk_salesorder
        , salesreasonid as fk_salesreason
    from {{ source('adventure_works_seeds','salesorderheadersalesreason') }}
)

select *
from source