with source as (
    select
    businessentityid as pk_person
    , addressid as pk_address
    , addresstypeid as pk_addresstype
    from {{ source('adventure_works_seeds','businessentityaddress') }}
)

select *
from source