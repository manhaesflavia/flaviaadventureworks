with source as (
    select
         salesreasonid as pk_salesreason
         , name as salesreason_name
         , reasontype
    from {{ source('adventure_works_seeds','salesreason') }}
)

select *
from source