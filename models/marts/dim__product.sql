with source as (
    select
        pk_product
        , product_name
        , safetystocklevel
        , reorderpoint
        , standardcost
        , listprice
        , daystomanufacture
        , productline
        , sell_startdate
        , sell_enddate
        , case
            when productsubcategory_name is null then 'Unknown'
            else productsubcategory_name
        end as productsubcategory_name      
        , case
            when productcategory_name is null then 'Unknown'
            else productcategory_name
        end as productcategory_name
    from {{ ref('int__productcategory') }}
),

-- Generate surrogate key
product_with_surrogate_key as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['pk_product']
        ) }} as sk_product
        , pk_product
        , product_name
        , safetystocklevel
        , reorderpoint
        , standardcost
        , listprice
        , daystomanufacture
        , productline
        , sell_startdate
        , sell_enddate
        , productsubcategory_name
        , productcategory_name
    from source
)

select *
from product_with_surrogate_key
