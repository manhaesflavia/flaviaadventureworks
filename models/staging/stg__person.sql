with
    source as (
        select
            businessentityid as pk_person
            , persontype
            , firstname || ' ' || lastname as full_name
            , case when emailpromotion = 0 then False
                   else True 
              end as is_emailpromotion
        from {{ source('adventure_works_seeds','person') }}
    )

select *
from source