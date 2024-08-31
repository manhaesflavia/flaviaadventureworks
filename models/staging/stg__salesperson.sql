with
 source as (
    select
      businessentityid as pk_sales_person
      , territoryid as fk_territory      
    from {{ source('adventure_works_seeds','salesperson') }}
)

select *
from source