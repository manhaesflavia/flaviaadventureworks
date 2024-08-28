with salesorderheader_customer as (
    select 
        so.pk_salesorder
        , so.order_date
        , so.is_onlineorderflag
        , so.fk_customer
        , c.pk_customer
    from 
        {{ ref('stg__salesorderheader') }} as so
    left join 
        {{ ref('stg__customer') }} as c 
    on 
        so.fk_customer = c.pk_customer
)

, salesorderdetail_product as (
    select 
        sod.fk_salesorder
        , sod.orderqty
        , sod.fk_product
        , p.sell_startdate
        , p.sell_enddate
        , p.product_name
    from 
        {{ ref('stg__salesorderdetail') }} as sod
    left join 
        {{ ref('stg__product') }} as p 
    on 
        sod.fk_product = p.pk_product
)

, store_info as (
    select 
        s.fk_customer
        , s.store_name
    from 
        {{ ref('stg__store') }} as s
)

select 
    sc.pk_salesorder
    , sc.order_date
    , sc.is_onlineorderflag      
    , sp.sell_startdate
    , sp.sell_enddate
    , sp.product_name
    , sp.orderqty
    , sp.fk_product
from 
    salesorderheader_customer as sc
left join 
    salesorderdetail_product as sp 
on 
    sc.pk_salesorder = sp.fk_salesorder
left join 
    store_info as si 
on 
    sc.fk_customer = si.fk_customer