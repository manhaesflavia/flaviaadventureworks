with source as (
    select
        addresstypeid as pk_addresstype
        , name as addresstype_name
    from {{ source('adventure_works_seeds','addresstype') }}
)

select *
from source