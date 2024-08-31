with distinct_salesorder as (
    select distinct
        io.pk_salesorder
        , io.sk_customer
        , io.subtotal
        , io.order_date
    from {{ ref('fct__salesorder') }} io
)

, customer_summary as (
    select
        sk_customer
        , count(*) as total_orders
        , sum(subtotal) as total_faturamento
        , min(order_date) as first_order_date
        , max(order_date) as last_order_date
    from distinct_salesorder
    group by sk_customer
)

select
    sk_customer
    , total_orders
    , total_faturamento
    , first_order_date
    , last_order_date
from customer_summary
order by total_orders desc