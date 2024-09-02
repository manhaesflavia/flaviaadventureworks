-- with merged_data as (
--     select
--         so.sk_salesorderdetail,
--         so.pk_salesorderdetail,
--         so.pk_salesorder,
--         so.fk_product,
--         so.orderqty,
--         so.is_onlineorderflag,
--         s.store_name,
--         so.fk_salesterritory,
--         so.order_date
--     from 
--         {{ ref('int__salesorderheader') }} as so
--     left join 
--         {{ ref('stg__store') }} as s 
--     on 
--         so.fk_salesperson = s.fk_person
-- ),

-- ranked_data as (
--     select
--         *,
--         row_number() over (partition by sk_salesorderdetail order by pk_salesorderdetail) as rn
--     from 
--         merged_data
-- ),

-- -- Join with dim__product to include product details
-- final_data as (
--     select
--         rd.sk_salesorderdetail,
--         rd.pk_salesorderdetail,
--         rd.pk_salesorder,
--         rd.fk_product,
--         rd.orderqty,
--         rd.is_onlineorderflag,
--         rd.store_name,
--         rd.fk_salesterritory,
--         rd.order_date,
--         p.product_name,
--         p.sell_startdate,
--         p.sell_enddate
--     from 
--         ranked_data rd
--     left join 
--         {{ ref('dim__product') }} p 
--     on 
--         rd.fk_product = p.pk_product
--     where 
--         rd.rn = 1
-- )

-- -- Select final columns for the fact table
-- select
--     sk_salesorderdetail,
--     pk_salesorderdetail,
--     pk_salesorder,
--     fk_product,
--     orderqty,
--     is_onlineorderflag,
--     store_name,
--     fk_salesterritory,
--     order_date,
--     product_name,
--     sell_startdate,
--     sell_enddate
-- from 
--     final_data

-- models/fct_salesorderdetail.sql

with merged_data as (
    select
        so.sk_salesorderdetail,
        so.pk_salesorderdetail,
        so.pk_salesorder,
        so.fk_product,
        so.orderqty,
        so.is_onlineorderflag,
        s.store_name,
        so.fk_salesterritory,
        so.order_date
    from 
        {{ ref('int__salesorderheader') }} as so
    left join 
        {{ ref('stg__store') }} as s 
    on 
        so.fk_salesperson = s.fk_person
),

ranked_data as (
    select
        *,
        row_number() over (partition by sk_salesorderdetail order by pk_salesorderdetail) as rn
    from 
        merged_data
),

-- Join with dim__product to include product details
product_data as (
    select
        rd.sk_salesorderdetail,
        rd.pk_salesorderdetail,
        rd.pk_salesorder,
        rd.fk_product,
        rd.orderqty,
        rd.is_onlineorderflag,
        rd.store_name,
        rd.fk_salesterritory,
        rd.order_date,
        p.product_name,
        p.sell_startdate,
        p.sell_enddate
    from 
        ranked_data rd
    left join 
        {{ ref('dim__product') }} p 
    on 
        rd.fk_product = p.pk_product
    where 
        rd.rn = 1
),

-- Join with dim__salesterritory to include sales territory details
final_data as (
    select
        pd.sk_salesorderdetail,
        pd.pk_salesorderdetail,
        pd.pk_salesorder,
        pd.fk_product,
        pd.orderqty,
        pd.is_onlineorderflag,
        pd.store_name,
        pd.fk_salesterritory,
        pd.order_date,
        pd.product_name,
        pd.sell_startdate,
        pd.sell_enddate,
        t.territory_name,
        t.fk_countryregioncode,
        t.territory_group
    from 
        product_data pd
    left join 
        {{ ref('dim__salesterritory') }} t 
    on 
        pd.fk_salesterritory = t.pk_salesterritory
)

-- Final selection for the fact table
select
    sk_salesorderdetail,
    pk_salesorderdetail,
    pk_salesorder,
    fk_product,
    orderqty,
    is_onlineorderflag,
    store_name,
    fk_salesterritory,
    order_date,
    product_name,
    sell_startdate,
    sell_enddate,
    territory_name,
    fk_countryregioncode,
    territory_group
from 
    final_data