with
    source as (
        select
            businessentityid as pk_person
            , case
                when persontype = 'SC' then 'Store Contact'
                when persontype = 'IN' then 'Individual (retail) customer'
                when persontype = 'SP' then 'Sales person'
                when persontype = 'EM' then 'Employee (non-sales)'
                when persontype = 'VC' then 'Vendor contact'
                when persontype = 'GC' then 'General contact'
            end as persontype
            , firstname || ' ' || lastname as full_name
            , case when emailpromotion = 0 then False
                   else True 
              end as is_emailpromotion
        from {{ source('adventure_works_seeds','person') }}
    )

select *
from source