with source as (
    select
        businessentityid as pk_person
        ,addressid as pk_address
        ,addresstypeid as pk_addresstype
        ,modifieddate
    from {{ source('adventure_works_seeds','businessentityaddress') }}
),

ranked_rows as (
    select
        *
        ,row_number() over (
            partition by pk_person
            order by modifieddate desc
        ) as row_num
    from source
)

select
    pk_person
    ,pk_address
    ,pk_addresstype
from ranked_rows
where row_num = 1