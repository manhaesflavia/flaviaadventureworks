with
 source as (
    select
      businessentityid as fk_customer
      , name as store_name
    from {{ source('adventure_works_seeds','store') }}
)

select *
from source