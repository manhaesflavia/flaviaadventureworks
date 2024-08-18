with source as (
    select
         salesreasonid as pk_salesreason
         , case
             when name = 'Television  Advertisement' then 'Television Advertisement'
             else name
         end as salesreason_name 
         , reasontype
    from {{ source('adventure_works_seeds','salesreason') }}
)

select *
from source