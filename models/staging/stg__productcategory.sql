with source as (
    select
        as pk_productsubcategory as pk_productcategory
        , name as productcategory_name
    from {{ source('adventure_works_seeds','productcategory') }}
)

select *
from source