with orders as (
    select
        pk_salesorder
        , fk_customer
        , fk_address
        , fk_salesterritory
        , is_onlineorderflag
        , order_status
        , FORMAT_TIMESTAMP('%d/%m/%Y', order_date) as order_date
        , subtotal
        , taxamt
        , freight
    from {{ ref('stg__salesorderheader') }}
),

orders_detail as (
    select
        pk_salesorderdetail
        , fk_salesorder
        , fk_product
        , orderqty
        , unitprice
        , unitpricediscount
    from {{ ref('stg__salesorderdetail') }}
),

partition_details_by_orders as (
    select
        o.pk_salesorder
        , row_number() over (partition by o.pk_salesorder order by od.pk_salesorderdetail) as row_order_details
        , od.unitprice * od.orderqty as total_order_value
        , od.orderqty
    from orders as o
    inner join orders_detail as od on o.pk_salesorder = od.fk_salesorder
),

sales_orders as (
    -- Calculando métricas por ordem
    select
        pk_salesorder
        , count(row_order_details) as qnt_products
        , sum(total_order_value) as total_order_value
        , sum(orderqty) as qnt_itens
    from partition_details_by_orders
    group by pk_salesorder
),

sales as (
    -- Mesclando e adicionando informações adicionais
    select
        o.pk_salesorder
        , o.order_date
        , o.fk_customer
        , o.fk_address
        , o.fk_salesterritory
        , o.is_onlineorderflag
        , o.order_status
        , o.subtotal
        , o.taxamt
        , o.freight
        , so.qnt_products
        , so.qnt_itens
        , so.total_order_value
    from orders as o
    left join sales_orders as so on o.pk_salesorder = so.pk_salesorder
)

select *
from sales