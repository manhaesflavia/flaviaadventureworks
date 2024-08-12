with source as (
    select
        productsubcategoryid as pk_productsubcategory
        , name as productsubcategory_name
    from {{ source('adventure_works_seeds','productsubcategory') }}
)

select *
from source