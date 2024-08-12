with source as (
    select
        productcategoryid as pk_productcategory
        , name as productcategory_name
    from {{ source('adventure_works_seeds','productcategory') }}
)

select *
from source