with product_dim as (
    select
        dp.pk_product,
        dp.sk_product
    from {{ ref('dim__product') }} dp
),

-- 1.2. Dimensão Cliente
customer_dim as (
    select
        dc.pk_customer,
        dc.sk_customer
    from {{ ref('dim__customer') }} dc
),

-- 1.3. Dimensão Sales Reason
salesreason_dim as (
    select
        dsr.fk_salesorder,
        dsr.sk_salesreason
    from {{ ref('dim__salesreason') }} dsr
),

-- 1.4. Dimensão Sales Territory
salesterritory_dim as (
    select
        dst.pk_salesterritory,
        dst.sk_salesterritory
    from {{ ref('dim__salesterritory') }} dst
),

-- 1.5. Dimensão Credit Card
creditcard_dim as (
    select
        dcc.fk_creditcard,
        dcc.sk_creditcard
    from {{ ref('dim__creditcard') }} dcc
),

-- 2. Unir as dimensões com a tabela intermediária, garantindo unicidade
final_salesorder as (
    select distinct
        io.sk_salesorderdetail,
        io.pk_salesorderdetail,
        io.fk_salesorder,
        prod.sk_product,
        io.orderqty,
        io.unitprice,
        io.unitpricediscount,
        io.pk_salesorder,
        cust.sk_customer,
        io.fk_address,
        terr.sk_salesterritory,
        card_.sk_creditcard,
        io.is_onlineorderflag,
        io.order_status,
        io.order_date,
        io.subtotal,
        io.taxamt,
        io.freight,
        io.total_order_value,
        reason.sk_salesreason
    from {{ ref('int__salesorderheader') }} io
    left join product_dim prod
        on io.fk_product = prod.pk_product
    left join customer_dim cust
        on io.fk_customer = cust.pk_customer
    left join salesreason_dim reason
        on io.fk_salesorder = reason.fk_salesorder
    left join salesterritory_dim terr
        on io.fk_salesterritory = terr.pk_salesterritory
    left join creditcard_dim card_
        on io.fk_creditcard = card_.fk_creditcard
)

-- 3. Selecionar os resultados finais para a tabela fato
select *
from final_salesorder