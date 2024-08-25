with source as (
    select
        pk_product,
        product_name,
        safetystocklevel,
        reorderpoint,
        standardcost,
        listprice,
        daystomanufacture,
        productline,
        sell_startdate,
        sell_enddate,
        case
            when productsubcategory_name is null then 'Unknown'
            else productsubcategory_name
        end as productsubcategory_name,
        case
            when productcategory_name is null then 'Unknown'
            else productcategory_name
        end as productcategory_name
    from {{ ref('int__productcategory') }}
),

-- generate surrogate key
product_with_surrogate_key as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['pk_product']
        ) }} as sk_product,
        pk_product,
        product_name,
        safetystocklevel,
        reorderpoint,
        standardcost,
        listprice,
        daystomanufacture,
        productline,
        sell_startdate,
        sell_enddate,
        productsubcategory_name,
        productcategory_name
    from source
),

-- join with stg__salesorderdetail to filter products
filtered_products as (
    select distinct
        p.sk_product,
        p.pk_product,
        p.product_name,
        p.safetystocklevel,
        p.reorderpoint,
        p.standardcost,
        p.listprice,
        p.daystomanufacture,
        p.productline,
        p.sell_startdate,
        p.sell_enddate,
        p.productsubcategory_name,
        p.productcategory_name
    from product_with_surrogate_key p
    join {{ ref('stg__salesorderdetail') }} sod
    on p.pk_product = sod.fk_product
)

select *
from filtered_products