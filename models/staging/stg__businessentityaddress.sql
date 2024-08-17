with source as (
    select
        businessentityid as fk_person
        , addressid as pk_address
        , addresstypeid as fk_addresstype
    from {{ source('adventure_works_seeds','businessentityaddress') }}
)

select *
from source