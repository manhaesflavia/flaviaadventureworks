with
 source as (
    select
      productid as fk_poduct
      , locationid as fk_location
      , quantity
    from {{ source('adventure_works_seeds','productinventory') }}
)

select *
from source