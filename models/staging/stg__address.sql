with
 source as (
    select
        /* Primary Key */      
        addressid as pk_address

        /* Foreign Key */        
        , stateprovinceid as fk_stateprovince
        , city
    from {{ source('adventure_works_seeds','address') }}
)

select *
from source