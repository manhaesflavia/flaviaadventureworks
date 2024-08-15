with
 source as (
    select    
        addressid as pk_address
        , stateprovinceid as fk_stateprovince
        , city
    from {{ source('adventure_works_seeds','address') }}
)

select *
from source