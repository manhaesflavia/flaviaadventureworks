with source as (
    select
         productid as pk_product
        , productsubcategoryid as fk_productsubcategory
        , name as product_name        
        , safetystocklevel
        , reorderpoint
        , standardcost
        , listprice
        , daystomanufacture
        , productline
        , sellstartdate
        , sellenddate
    from {{ source('adventure_works_seeds', 'product') }}
)

select *
from source