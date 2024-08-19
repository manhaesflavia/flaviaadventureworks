with stg__product as (
    select *
    from {{ ref('stg__product') }}
),

stg__productsubcategory as (
    select *
    from {{ ref('stg__productsubcategory') }}
),

stg__productcategory as (
    select *
    from {{ ref('stg__productcategory') }}
),

product_with_subcategory as (
    select 
        stg__product.*, 
        stg__productsubcategory.*
    from stg__product
    left join stg__productsubcategory 
        on stg__product.fk_productsubcategory = stg__productsubcategory.pk_productsubcategory
),

product_with_category as (
    select 
        product_with_subcategory.*, 
        stg__productcategory.*
    from product_with_subcategory
    left join stg__productcategory 
        on product_with_subcategory.fk_productcategory = stg__productcategory.pk_productcategory
)

select 
    product_with_category.* except(pk_productsubcategory, pk_productcategory)
from product_with_category